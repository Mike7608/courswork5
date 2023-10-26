import psycopg2
from setting import settings


class DBManager:
    """
    Класс для работы с базой данных
    """
    @staticmethod
    def is_exists_db() -> bool:
        """
        Процедура проверяет наличие базы данных
        :return: Возвращает True если БД имеется, иначе False
        """
        try:
            conn = psycopg2.connect(
                host=settings.HOST,
                database=settings.DATABASE,
                user=settings.USER,
                password=settings.PASSWORD
            )
            print("Соединение с базой данных успешно установлено.")
            cur = conn.cursor()
            cur.close()
            conn.close()
            return True
        except psycopg2.OperationalError:
            print("Не удалось установить соединение с базой данных.")
            return False

    @staticmethod
    def is_exists_data() -> bool:
        """
        Проверяет наличие данных в БД
        :return: Возвращает True или False
        """
        conn = psycopg2.connect(
            host=settings.HOST,
            database=settings.DATABASE,
            user=settings.USER,
            password=settings.PASSWORD
        )

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM vacancies")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()

        if count > 0:
            print("В базе данных присутствуют данные.")
            return True
        else:
            return False

    @staticmethod
    def clear_data() -> None:
        """
        Процедура очищает таблицы БД и сбрасывает счетчики полей
        :return: пустая БД
        """
        conn = psycopg2.connect(
            host=settings.HOST,
            database=settings.DATABASE,
            user=settings.USER,
            password=settings.PASSWORD
        )
        cursor = conn.cursor()

        cursor.execute("truncate table areas restart identity")
        cursor.execute("truncate table employers restart identity cascade")

        conn.commit()
        cursor.close()
        conn.close()
        print("База данных очищена!")

    @staticmethod
    def get_data(sql_string: str) -> list:
        """
        Получает данные из БД согласно переданного запроса
        :param sql_string: запрос
        :return: список данных
        """
        conn = psycopg2.connect(
            host=settings.HOST,
            database=settings.DATABASE,
            user=settings.USER,
            password=settings.PASSWORD
        )
        cursor = conn.cursor()
        cursor.execute(sql_string)
        data = cursor.fetchall()

        cursor.close()
        conn.close()
        return data

    @staticmethod
    def get_companies_and_vacancies_count():
        """
        получает список всех компаний и количество вакансий у каждой компании.
        :return: список всех компаний и количество вакансий
        """
        sql_string = ("select employers.name, count(vacancies.employer_id) as total from employers "
                      "inner join vacancies on vacancies.employer_id = employers.id "
                      "group by employers.id")
        return DBManager.get_data(sql_string)

    @staticmethod
    def get_all_vacancies():
        """
         получает список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию.
        :return: список всех вакансий
        """
        sql_string = ("select vacancies.name, employers.name as employer, vacancies.salary_from, vacancies.salary_to, "
                      "vacancies.currency, vacancies.url from vacancies "
                      "inner join employers on vacancies.employer_id = employers.id")
        return DBManager.get_data(sql_string)

    @staticmethod
    def get_avg_salary(currency: str = 'RUR'):
        """
        получает среднюю зарплату по вакансиям. Внимание! В расчете не участвуют вакансии с нулевой зарплатой.
        :param currency: тип валюты. Например: RUR
        :return: среднюю зарплату по вакансиям
        """
        sql_string = (f"select salary_from, salary_to from vacancies "
                      f"where currency = '{currency}' and salary_from >0 or salary_to >0")
        data = DBManager.get_data(sql_string)

        summ = 0

        for row in data:
            summ += DBManager.get_avg_row_salary(row[0], row[1])

        return round(summ / len(data), 2)

    @staticmethod
    def get_avg_row_salary(s_from, s_to):
        """
        Процедура расчёта средней з.п.
        :param s_from: зарплата ОТ
        :param s_to: зарплата ДО
        :return: среднее значение
        """
        if s_from is None:
            s_from = 0
        if s_to is None:
            s_to = 0

        if s_from > 0 and s_to == 0:
            result = s_from
        elif s_from == 0 and s_to > 0:
            result = s_to / 2
        else:
            if s_from > 0 and s_to > 0:
                result = (s_from + s_to) / 2
            else:
                result = None
        return result

    @staticmethod
    def get_vacancies_with_higher_salary(currency: str = 'RUR'):
        """
        получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.
        :param currency: тип валюты. Например: RUR
        :return: список всех вакансий
        """
        avg_salary = DBManager.get_avg_salary(currency)

        sql_string = (f"select vacancies.*, areas.name as area, employers.name as employer "
                      f"from vacancies "
                      f"inner join areas on vacancies.area_id = areas.id "
                      f"inner join employers on vacancies.employer_id = employers.id "
                      f"where (salary_from > 0 or salary_to > 0) and currency = '{currency}'")

        data = DBManager.get_data(sql_string)

        data_new = []

        for row in data:
            if DBManager.get_avg_row_salary(row[3], row[4]) > avg_salary:
                # data_temp = {'id': row[0], 'name': row[1], 'area': row[2], 'salary_from': row[3], 'salary_to': row[4],
                #              'currency': row[5], 'url': row[6], 'description': row[7], 'employer_id': row[8]}
                data_new.append(row)
        return data_new

    @staticmethod
    def get_vacancies_with_keyword(words: str):
        """
        получает список всех вакансий, в названии которых содержатся переданные в метод слова, например python.
        :param words: слова для поиска и отборки вакансий
        :return: список всех вакансий
        """
        list_words = words.split(" ")
        string_add_sql = ""
        index = 0
        for item in list_words:
            string_add_sql += f"vacancies.description like '%{item}%' or vacancies.name like '%{item}%'"
            index += 1
            if index < len(list_words):
                string_add_sql += " and "
        sql_string = (f"select vacancies.*, areas.name as area, employers.name as employer "
                      f"from vacancies "
                      f"inner join areas on vacancies.area_id = areas.id "
                      f"inner join employers on vacancies.employer_id = employers.id "
                      f"where " + string_add_sql)

        return DBManager.get_data(sql_string)
