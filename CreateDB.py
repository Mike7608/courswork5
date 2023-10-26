import psycopg2
from setting import settings


class NewDataBase:
    """
    Класс для создания новой БД
    """
    def __init__(self):
        self.name = settings.DATABASE
        self.password = settings.PASSWORD
        self.host = settings.HOST
        self.user = settings.USER

    def create_db(self):
        """
        Процедура создания БД
        :return:
        """
        # Установление соединения с PostgreSQL
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password)

        conn.autocommit = True
        # Создание объекта-курсора
        cursor = conn.cursor()

        try:
            # Создание SQL-запроса для создания базы данных
            database_name = self.name
            create_database_query = f"CREATE DATABASE {database_name};"
            # Выполнение SQL-запроса
            cursor.execute(create_database_query)
            print("База данных успешно создана.")

            # Подтверждение изменений
            conn.commit()
        except psycopg2.Error as e:
            print("Ошибка при создании базы данных:", str(e))
        finally:
            # Закрытие курсора и соединения
            cursor.close()
            conn.close()

    def create_tables(self):
        """
        Процедура создания таблиц в БД
        :return:
        """
        conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.name)
        cursor = conn.cursor()

        # создаем таблицу employers
        cursor.execute("CREATE TABLE employers ("
                       "id integer NOT NULL, name varchar, url varchar, description varchar, "
                       "CONSTRAINT employers_pkey PRIMARY KEY (id))")
        print("Таблица employers создана.")

        # создаем таблицу vacancies
        cursor.execute("CREATE TABLE vacancies ("
                       "id serial NOT NULL, "
                       "name varchar,"
                       "area_id integer, "
                       "salary_from integer, "
                       "salary_to integer, "
                       "currency varchar(3), "
                       "url varchar, "
                       "description varchar, "
                       "employer_id integer NOT NULL, "
                       "CONSTRAINT vacancies_pkey PRIMARY KEY (id), "
                       "CONSTRAINT fk_employer_id FOREIGN KEY (employer_id) "
                       "REFERENCES employers (id) MATCH SIMPLE "
                       "ON UPDATE NO ACTION "
                       "ON DELETE CASCADE "
                       "NOT VALID)")
        print("Таблица vacancies создана.")

        # создаем таблицу countries
        cursor.execute("CREATE TABLE areas (id integer NOT NULL, parent_id integer, name character varying, "
                       "CONSTRAINT areas_pkey PRIMARY KEY (id))")

        print("Таблица areas создана.")

        # Подтверждение изменений
        conn.commit()
        cursor.close()
        conn.close()
