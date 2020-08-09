import csv
import re
from datetime import datetime

from pymongo import MongoClient
import pymongo


client = MongoClient(host='mongodb+srv://Nataliya13:tvoug13777T@cluster0.3bhge.mongodb.net/test')
artists_db1 = client['artists']
conserts = artists_db1['conserts']

def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        data_srv = list(reader)
        new_list = []
        for data in data_srv:
            d_o = datetime.strptime(data['Дата'] + '.2020', '%d.%m.%Y')
            d = dict(Исполнитель=data['Исполнитель'], Цена=int(data['Цена']), Место=data['Место'], Дата=[d_o])
            new_list.append(d)
        conserts.insert_many(new_list)


def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастанию цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    all_conserts = conserts.find().sort('Цена', 1)
    for c in all_conserts:
        print(c)


def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке, например "Seconds to"),
    и вернуть их по возрастанию цены
    """
    regex = re.compile('укажите регулярное выражение для поиска. ' \
                       'Обратите внимание, что в строке могут быть специальные символы, их нужно экранировать')

    regex = re.compile(name, re.IGNORECASE)
    for c in conserts.find({'Исполнитель': regex}).sort('Цена', pymongo.ASCENDING):
        print(c)


def find_early_date(date_start, date_end, db):
    date = datetime.strptime(date_start, '%d.%m.%Y')
    date1 = datetime.strptime(date_end, '%d.%m.%Y')
    for c in conserts.find({'Дата': {'$elemMatch': {'$gte': date, '$lte': date1}}}).sort('Дата', 1):
        print(c['Исполнитель'], c['Дата'][0].strftime('%d.%m.%Y'))


if __name__ == '__main__':
    #read_data('artists.csv', artists_db1)
    #find_cheapest(artists_db1)
    #conserts.drop()
    #find_by_name('Hit Music', artists_db1)
    find_early_date('01.05.2020', '30.07.2020', artists_db1)