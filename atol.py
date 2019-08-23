import datetime
import os
import pathlib
import sys
import time
import click
import requests
import extract_msg
import xmltodict
import logging
import psycopg2


class ChecksPlease:

    def __init__(self, inn, pay_address, group, shop_id, login, password, folder, cert_path, key_path,
                 db_connection_string,db_connection_string2):
        self.logger = logging.getLogger()
        self.db_connection_string = db_connection_string
        self.db_connection_string2 = db_connection_string2
        logging.basicConfig(filename="atol.log", format='%(asctime)s %(levelname)-8s %(message)s ', filemode='a')
        self.logger.setLevel(logging.WARN)
        self.session = requests.session()
        self.inn = inn
        self.tid =None
        self.cert_path = cert_path
        self.key_path = key_path
        self.folder = folder
        self.shop_id = shop_id
        self.pay_address = pay_address
        self.group = group
        self.db = psycopg2.connect(self.db_connection_string)
        self.db_user = psycopg2.connect(self.db_connection_string2)
        self.session.headers.update({'Content-type': 'application/json; charset=utf-8'})
        if self.atol_auth(login, password) == -1: sys.exit()

    def atol_auth(self, login, passwd):
        res = self.session.get(f'https://online.atol.ru/possystem/v4/getToken?login={login}&pass={passwd}')
        if res.json()['error'] is not None:
            self.logger.error('Не удалось авторизоваться в Атол')
            return -1
        self.session.headers.update({'Token': res.json()['token'], 'Content-type': 'application/json; charset=utf-8'})
        self.logger.warning('Успешная авторизация в Атол')

    def parse_letter(self, filename):
        self.logger.warning(f'Начата обработка файла {filename}')
        msg = extract_msg.Message(filename)

        for line in msg.body.split('\n'):
            if 'Номер транзакции' in line:
                tid = line.split(':')[-1].strip()
            if 'Идентификатор запроса к онлайн-кассе' in line:
                rid = line.split(':')[-1].strip()
                msg.close()
                return tid,rid
        msg.close()
        raise ValueError(f'Не найден номер транзакции в файле {filename}')

    def ya_list_order(self, id):
        now = datetime.datetime.now()
        format_iso_now = now.isoformat()
        d = {
            'shopId': self.shop_id,
            'invoiceId': id,
            'requestDT': format_iso_now,
            'outputFormat': 'XML'
        }
        try:
            res = requests.post('https://penelope.yamoney.ru/webservice/mws/api/listOrders', data=d,
                                cert=(self.cert_path, self.key_path),
                                verify=False)
            return xmltodict.parse(res.text)
        except:
            raise ValueError('Ошибка при получении данных из Яндекс кассы', res.text)


    def register_check(self, ya_order,tid_suff=0):
        if ya_order['listOrdersResponse']['@error'] != '0':
            raise ValueError('Не удалось получить данные из яндекс кассы', ya_order)
        sum = float(ya_order['listOrdersResponse']['order']['@orderSumAmount'])
        js = {
            "receipt": {
                "items": [
                    {
                        "sum": sum,
                        "vat": {
                            "type": "none"
                        },
                        "name": """Балл Антиплагиата""",
                        "price": 1,
                        # price = 1 так как унас покупается n баллов за 1 рубль
                        "quantity": sum,
                        "payment_method": "full_payment",
                        "payment_object": "intellectual_activity"
                    }
                ],
                "total": sum,
                "client": {
                    "email": "" #заглушка
                },
                "company": {
                    "inn": self.inn,
                    "sno": "osn",
                    "email": "support@antiplagiat.ru",
                    "payment_address": self.pay_address
                },
                "payments": [
                    {
                        "sum": sum,
                        "type": 1
                    }
                ]
            },
            "timestamp": datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S"),
            "external_id": str(self.tid)+'_00'* tid_suff
             #ЯК добавляет суффикс _00 к идентификатору запроса к онлайн-кассе
            # При повторной отправке
        }
        print(str(self.tid) + '_00' * tid_suff)
        res = self.session.post(f"https://online.atol.ru/possystem/v4/{self.group}/sell", json=js)
        print(res.text)
        if res.json()['status'] == 'fail':
            raise ValueError('Ошибка при отправке чека на регистрацию', res.text)
        #elif res.json()['status'] == 'wait' and res.json()['error']['code']==33:
        #    raise ValueError('Чек уже зарегистрирован', res.text)
        else:
            return res.json()['uuid']

    def get_registration_status(self, uuid):
        for i in range(0, 5):
            res = self.session.get(
                f"https://online.atol.ru/possystem/v4/{self.group}/report/{uuid}").json()
            print(res)
            status = res['status']
            #Возможно чек зарегистрирован но с ошибкой
            #ЯК добавляет суффикс _00 к идентификатору запроса к онлайн - кассе при повторной отправке
            # Попробуем отправить еще раз с таким суфиксом в ExternalId
            #Здесь рассмотерны только случаи ошибки Таймаут сообщения в очереди и
            # "Документ не может быть обработан данной ККТ, так как в ее группе не разрешен этот адрес расчёта"
            if res['status']=='fail' and (res['error']['code']==2 or res['error']['code']==1):
                return -1
            elif res['status']=='fail':
                raise ValueError('Ошибка в отправленом на регистрацию документе документе', res)
            if status == 'wait':
                time.sleep(15)
            elif status == 'done':
                return 1
        raise ValueError('Не удалось получить статус done. Файл пропущен', res)

    def parse_folder(self):
        good = 0
        bad = 0
        for filepath in pathlib.Path(self.folder).glob('**/*'):
            if not os.path.isfile(filepath): continue
            try:
                tid_suff=0
                while True:
                    id,self.tid = self.parse_letter(filepath)
                    order_details = self.ya_list_order(id)
                    reg_result = self.register_check(order_details)
                    reg_status = self.get_registration_status(reg_result)
                    if reg_status==-1:
                        tid_suff +=1
                        reg_result = self.register_check(order_details,tid_suff)
                        reg_status = self.get_registration_status(reg_result)
                    if reg_status!=-1:
                        self.logger.warning(f'Файл {filepath} успешно обработан')
                        os.remove(filepath)
                        good = good + 1
                        break
            except Exception as e:
                self.logger.error(f'Ошибка при обработке файла {filepath}')
                self.logger.exception(e)
                bad = bad + 1
                continue
        self.logger.warning(f'Обработано успешно - {good}')
        self.logger.warning(f'Не обработаны  - {bad}')
        self.db.close()
        self.db_user.close()


@click.command()
@click.option('--inn', prompt='ИНН организации', help='ИНН организации')
@click.option('--shop_id', prompt='Идентификатор магазина в яндекс кассе',
              help='Идентификатор магазина в яндекс кассе')
@click.option('--pay_address', prompt='Место расчетов в Атол', help='Место расчетов В Атол')
@click.option('--group', prompt='Код группы в Атол', help='Код группы в Атол')
@click.option('--login', prompt='Логин Атол', help='Логин Атол')
@click.option('--password', prompt='Пароль Атол', help='Пароль Атол')
@click.option('--folder', prompt='Папка с письмами', help='Папка с письмами')
@click.option('--cert_path', prompt='Путь к сертификату Яндекс кассы', help='Путь к сертификату Яндекс кассы')
@click.option('--key_path', prompt='Путь к ключу сертификата Яндекс кассы',
              help='Путь к ключу сертификата Яндекс кассы')

@click.pass_context
def cli(ctx, inn, pay_address, group, shop_id, login, password, folder, cert_path, key_path):
    ctx.obj = ChecksPlease(inn, pay_address, group, shop_id, login, password, folder, cert_path, key_path)
    ctx.obj.parse_folder()


if __name__ == "__main__":
    cli()
