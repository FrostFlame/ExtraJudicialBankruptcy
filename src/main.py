import gzip
import matplotlib.pyplot as plt
import numpy as np
import os.path
import sqlite3
import xml.etree.ElementTree as et

from dadata import Dadata
from sqlite3 import Error


DADATA_API_KEY = '12f77383d2854aeb2bbb96ee3abed6f265ea27ee'
DB_FILE = 'judicialsqlite.db'

class DBCM:
    """Контекстный менеджер для БД."""

    def __init__(self, filename):
        self.filename = filename
        self.conn = None

    def __enter__(self):
        try:
            already_exists = os.path.isfile(self.filename)
            self.conn = sqlite3.connect(self.filename)
            if not already_exists:
                # Создание таблиц в случае их не существования
                self.__fill_db()
            return self.conn.cursor()
        except Error as e:
            print(e)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def __fill_db(self):
        cursor = self.conn.cursor()

        # Должники
        cursor.execute(
            '''
            CREATE TABLE debtor(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(20),
                birth_date DATETIME,
                birth_place VARCHAR(100),
                postal_code INTEGER,
                region VARCHAR(60),
                area VARCHAR(60),
                city VARCHAR(60),
                settlement VARCHAR(60),
                street VARCHAR(60),
                house VARCHAR(10),
                flat VARCHAR(10),
                inn VARCHAR(12),
                snils VARCHAR(11)
            )
            '''
        )

        # История имён должников
        cursor.execute(
            '''
            CREATE TABLE name_history(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                debtor_id INTEGER,
                value VARCHAR(20),
                FOREIGN KEY (debtor_id) REFERENCES debtor(id)
            )
            '''
        )

        # Авторы сообщений
        cursor.execute(
            '''
            CREATE TABLE publisher(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(50),
                inn VARCHAR(12),
                ogrn VARCHAR(13)
            )
            '''
        )

        # Банки
        cursor.execute(
            '''
            CREATE TABLE bank(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(60),
                bik VARCHAR(9)
            )
            '''
        )

        # Сообщения
        cursor.execute(
            '''
            CREATE TABLE message(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                uuid UUID,
                number INTEGER,
                type VARCHAR(60),
                publish_date DATE,
                debtor_id INTEGER,
                publisher_id INTEGER,
                finish_reason VARCHAR(200),
                FOREIGN KEY (debtor_id) REFERENCES debtor(id),
                FOREIGN KEY (publisher_id) REFERENCES publisher(id)
            )
            '''
        )

        # Связь банков и сообщений
        cursor.execute(
            '''
            CREATE TABLE message_bank(
                message_id INTEGER,
                bank_id INTEGER,
                FOREIGN KEY (message_id) REFERENCES message(id),
                FOREIGN KEY (bank_id) REFERENCES bank(id)
            )
            '''
       )

        # Денежные обязательства
        cursor.execute(
            '''
            CREATE TABLE monetary_obligation(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                creditor_name VARCHAR(60),
                content VARCHAR(60),
                basis VARCHAR(200),
                total_sum DECIMAL(9, 2),
                debt_sum DECIMAL(9, 2),
                message_id INTEGER,
                FOREIGN KEY (message_id) REFERENCES message(id)
            )
            '''
        )

        # Обязательные платежи
        cursor.execute(
            '''
            CREATE TABLE obligatory_payment(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(200),
                sum DECIMAL(9, 2),
                penalty_sum DECIMAL(9, 2),
                is_from_enterpreneurship BOOLEAN,
                message_id INTEGER,
                FOREIGN KEY (message_id) REFERENCES message(id)
            )
            '''
        )
        self.conn.commit()


def get_bank_data(bank):
    return (
        bank.find('Name').text,
        bank.find('Bik').text if bank.find('Bik') is not None else None
    )


def get_debtor_data(debtor):
    postal_code = None
    region = None
    area = None
    city = None
    settlement = None
    street = None
    house = None
    flat = None

    # Апишка, которая парсит адреса за меня
    dadata_api = Dadata(DADATA_API_KEY)
    result = dadata_api.suggest('address', debtor.find('Address').text)

    if result:
        result = result[0].get('data')
        postal_code = result.get('postal_code')
        region = result.get('region_with_type')
        area = result.get('area_with_type')
        city = result.get('city_with_type')
        settlement = result.get('settlement_with_type')
        street = result.get('street_with_type')
        house = result.get('house')
        flat = result.get('flat')
    return (
        debtor.find('Name').text,
        debtor.find('BirthDate').text,
        debtor.find('BirthPlace').text,
        postal_code,
        region,
        area,
        city,
        settlement,
        street,
        house,
        flat,
        debtor.find('Inn').text if debtor.find('Inn') is not None else None,
        debtor.find('Snils').text if debtor.find('Snils') is not None else None
    )


def get_publisher_data(publisher):
    return (
        publisher.find('Name').text,
        publisher.find('Inn').text,
        publisher.find('Ogrn').text
    )


def get_name_history_data(history, parent_map, db_connection):
    parent = parent_map[parent_map[history]]
    parent_id = db_connection.execute(
        '''
        SELECT id FROM debtor 
        WHERE name LIKE ? AND birth_date LIKE ?
        ''',
        (parent.find('Name').text, parent.find('BirthDate').text)).fetchone()[0]
    return parent_id, history.find('Value').text


def get_message_data(message, db_connection):
    message_id = message.find('Id').text
    number = message.find('Number').text
    message_type = message.find('Type').text
    publish_date = message.find('PublishDate').text
    debtor = message.find('Debtor')
    publisher = message.find('Publisher')
    debtor_id = db_connection.execute(
        '''
        SELECT id FROM debtor
        WHERE name LIKE ? AND birth_date LIKE ?
        ''',
        (debtor.find('Name').text, debtor.find('BirthDate').text)
    ).fetchone()[0]
    publisher_id = db_connection.execute(
        '''
        SELECT id FROM publisher
        WHERE name LIKE ? AND inn LIKE ? AND ogrn LIKE ?
        ''',
        (
            publisher.find('Name').text,
            publisher.find('Inn').text,
            publisher.find('Ogrn').text
        )
    ).fetchone()[0]
    finish_reason = message.find('FinishReason').text if (
        message.find('FinishReason') is not None) else None
    return (
        message_id,
        number,
        message_type,
        publish_date,
        debtor_id,
        publisher_id,
        finish_reason
    )


def get_message_bank_data(bank, parent_map, db_connection):
    message_id = db_connection.execute(
        '''
        SELECT id FROM message
        WHERE uuid = ?
        ''',
        (parent_map[parent_map[bank]].find('Id').text,)
    ).fetchone()[0]
    bank_id = db_connection.execute(
        '''
        SELECT id FROM bank
        WHERE name LIKE ? AND bik = ?
        ''',
        (bank.find('Name').text, bank.find('Bik').text)
    ).fetchone()[0] if bank.find('Bik') is not None else\
    db_connection.execute(
        '''
        SELECT id FROM bank
        WHERE name LIKE ? AND bik IS NULL
        ''',
        (bank.find('Name').text,)
    ).fetchone()[0]
    return message_id, bank_id


def get_obligation_data(obligation, parent_map, db_connection):
    message_id = db_connection.execute(
        '''
        SELECT id FROM message
        WHERE uuid = ?
        ''',
        (parent_map[parent_map[parent_map[obligation]]].find('Id').text,)
    ).fetchone()[0]
    return (
        obligation.find('CreditorName').text,
        obligation.find('Content').text,
        obligation.find('Basis').text,
        obligation.find('TotalSum').text,
        obligation.find('DebtSum').text if obligation.find('DebtSum') is not None else None,
        message_id)


def get_payment_data(payment, parent_map, db_connection):
    message_id = db_connection.execute(
        '''
        SELECT id FROM message
        WHERE uuid = ?
        ''',
        (parent_map[parent_map[parent_map[payment]]].find('Id').text,)
    ).fetchone()[0]
    is_from_enterpreneurship = (
        parent_map[parent_map[payment]].tag == 'CreditorsFromEntrepreneurship'
    )
    return (
        payment.find('Name').text,
        payment.find('Sum').text,
        payment.find('PenaltySum').text if payment.find('PenaltySum') is not None else None,
        is_from_enterpreneurship,
        message_id
    )


def part_1():
    """Python."""
    with gzip.open('ExtrajudicialData.xml.gz', 'r') as file:
        tree = et.parse(file)

        parent_map = {child: parent for parent in tree.iter() for child in parent}

    with DBCM(DB_FILE) as db:
        bank_records = set()
        for bank in tree.iter('Bank'):
            bank_records.add(get_bank_data(bank))
        db.executemany(
            '''
            INSERT INTO bank
            (name, bik)
            VALUES (?, ?)
            ''',
            bank_records
        )

        debtor_records = set()
        for debtor in tree.iter('Debtor'):
            debtor_records.add(get_debtor_data(debtor))
        db.executemany(
            '''
            INSERT INTO debtor
            (name, birth_date, birth_place, postal_code, region, area, city, settlement,
            street, house, flat, inn, snils)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            debtor_records
        )

        publisher_records = set()
        for publisher in tree.iter('Publisher'):
            publisher_records.add(get_publisher_data(publisher))
        db.executemany(
            '''
            INSERT INTO publisher
            (name, inn, ogrn)
            VALUES (?, ?, ?)
            ''',
            publisher_records
        )

        history_records = set()
        for history in tree.iter('PreviousName'):
            history_records.add(get_name_history_data(history, parent_map, db))
        db.executemany(
            '''
            INSERT INTO name_history
            (debtor_id, value)
            VALUES (?, ?)
            ''',
            history_records
        )

        message_records = []
        for elem in tree.iter('ExtrajudicialBankruptcyMessage'):
            message_records.append(get_message_data(elem, db))
        db.executemany(
            '''
            INSERT INTO message
            (uuid, number, type, publish_date, debtor_id, publisher_id, finish_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''',
            message_records
        )

        message_bank_records = []
        for bank in tree.iter('Bank'):
            message_bank_records.append(get_message_bank_data(bank, parent_map, db))
        db.executemany(
            '''
            INSERT INTO message_bank
            (message_id, bank_id)
            VALUES (?, ?)
            ''',
            message_bank_records
        )

        obligation_records = []
        for obligation in tree.iter('MonetaryObligation'):
            obligation_records.append(get_obligation_data(obligation, parent_map, db))
        db.executemany(
            '''
            INSERT INTO monetary_obligation
            (creditor_name, content, basis, total_sum, debt_sum, message_id)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            obligation_records
        )

        payment_records = []
        for payment in tree.iter('ObligatoryPayment'):
            payment_records.append(get_payment_data(payment, parent_map, db))
        db.executemany(
            '''
            INSERT INTO obligatory_payment
            (name, sum, penalty_sum, is_from_enterpreneurship, message_id)
            VALUES (?, ?, ?, ?, ?)
            ''',
            payment_records
        )


def part_2():
    """SQL."""

    with DBCM(DB_FILE) as db:
        print(db.execute(
            '''
            SELECT debtor.name, inn, COUNT(*) AS "Обязательства"
            FROM debtor
            LEFT JOIN message ON message.debtor_id = debtor.id
            LEFT JOIN monetary_obligation ON monetary_obligation.message_id = message.id
            GROUP BY debtor.name, inn
            ORDER BY Обязательства DESC, debtor.name
            LIMIT 10
            ''').fetchall())

        print(db.execute(
            '''
            SELECT debtor.name, inn, SUM(debt_sum) AS "Сумма"
            FROM debtor
            LEFT JOIN message ON message.debtor_id = debtor.id
            LEFT JOIN monetary_obligation ON monetary_obligation.message_id = message.id
            GROUP BY debtor.name, inn
            ORDER BY Сумма DESC, debtor.name
            LIMIT 10
            ''').fetchall())

        print(db.execute(
            '''
            SELECT debtor.name, inn,
            ROUND(MAX(0, SUM(total_sum - debt_sum)) / SUM(total_sum) * 100, 2) AS "Процент"
            FROM debtor
            LEFT JOIN message ON message.debtor_id = debtor.id
            LEFT JOIN monetary_obligation ON monetary_obligation.message_id = message.id
            GROUP BY debtor.name, inn
            HAVING Процент IS NOT NULL
            ORDER BY Процент ASC, debtor.name
            ''').fetchall())


def part_3():
    """Визуализация."""

    with DBCM(DB_FILE) as db:
        data = db.execute(
            '''
            SELECT region, IFNULL(ROUND(SUM(debt_sum), 2), 0)
            FROM debtor
            INNER JOIN message ON message.debtor_id = debtor.id
            INNER JOIN monetary_obligation ON monetary_obligation.message_id = message.id
            GROUP BY region
            ORDER BY region DESC
            '''
        ).fetchall()

        y_pos = np.arange(len(data))
        plt.barh(y_pos, [elem[1] for elem in data])
        plt.yticks(y_pos, [elem[0] for elem in data])
        plt.ticklabel_format(style='plain', axis='x', scilimits=(0, 0))

        plt.show()

        data = db.execute(
            '''
            SELECT (DATE() - birth_date) / 10 * 10 AS Возраст,
            IFNULL(ROUND(SUM(debt_sum), 2), 0)
            FROM debtor
            INNER JOIN message ON message.debtor_id = debtor.id
            INNER JOIN monetary_obligation ON monetary_obligation.message_id = message.id
            GROUP BY Возраст
            ORDER BY Возраст
            '''
        ).fetchall()

        y_pos = np.arange(len(data))
        plt.bar(y_pos, [elem[1] for elem in data])
        plt.xticks(y_pos, [elem[0] for elem in data])
        plt.ticklabel_format(style='plain', axis='y', scilimits=(0, 0))

        plt.show()


if __name__ == '__main__':
    part_1()
    part_2()
    part_3()
