import psycopg2
import hhAPI
from setting import settings


class FillDataBase:
    """
    Класс для заполнения БД данными
    """
    def __init__(self):
        self.name = settings.DATABASE
        self.password = settings.PASSWORD
        self.host = settings.HOST
        self.user = settings.USER

    def fill_areas(self) -> None:
        """
        Процедура заполняет таблицу areas (регионы)
        :return:
        """
        hh = hhAPI.HeadHunterAPI(settings.MAIN_URL)
        data_areas = hh.get_areas()

        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.name)
        cursor = conn.cursor()

        sql_string = "INSERT INTO areas (id, parent_id, name) VALUES (%s, %s, %s)"

        for country in data_areas:

            row_ins = (country['id'], country['id'], country['name'])
            cursor.execute(sql_string, row_ins)
            for region in country["areas"]:
                row_ins = (region['id'], country['id'], region['name'])
                cursor.execute(sql_string, row_ins)
                for city in region["areas"]:
                    row_ins = (city['id'], region['id'], city['name'])
                    cursor.execute(sql_string, row_ins)

        print("Таблица areas загружена")

        conn.commit()
        cursor.close()
        conn.close()

    def fill_data(self, id_company: int) -> None:
        """
        Процедура заполняет таблицы employers и vacancies
        :param id_company: код работодателя (employer)
        :return:
        """
        print(f"Ждите! Идет загрузка данных по коду [{id_company}]", end='')

        hh = hhAPI.HeadHunterAPI(settings.MAIN_URL)
        data = hh.get_vacancies(id_company)

        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.name)
        cursor = conn.cursor()

        # сначала вносим данные о работодателе
        data_emp = hh.get_employer(id_company)

        data_row = (data_emp['id'], data_emp['name'], data_emp['alternate_url'], data_emp['industries'][0]['name'])

        cursor.execute(f"INSERT INTO employers (id, name, url, description) "
                       f"VALUES (%s, %s, %s, %s)", data_row)

        # затем вносим все вакансии данного работодателя
        for row in data:

            salary = row['salary']

            if salary is None:
                salary = {'from': None, 'to': None, 'currency': None}

            ins_row = (row['name'], row['area']['id'], salary['from'], salary['to'], salary['currency'],
                       row['alternate_url'], row['snippet']['requirement'], id_company)

            cursor.execute(f"INSERT INTO vacancies (name, area_id, salary_from, salary_to, currency, url, "
                           f"description, employer_id) "
                           f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", ins_row)

        conn.commit()
        cursor.close()
        conn.close()

        print(f"'{data_emp['name']}'. Готово!")
