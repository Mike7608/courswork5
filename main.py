import db_manager
from db_manager import DBManager
from CreateDB import NewDataBase
from fillDB import FillDataBase
from setting import settings


def fill_data():
    """
    Процедура заполнения БД всеми требуемыми данными
    """
    fdb = FillDataBase()
    fdb.fill_areas()
    for item in settings.LIST_COMPANY:
        fdb.fill_data(item)
    print("Все данные загружены!")


# Если БД не найдена, тогда создаем новую
if not DBManager.is_exists_db():
    result = input("Хотите создать новую базу данных? (Y/N): ")
    if result.upper() == 'Y':
        newDB = NewDataBase()
        newDB.create_db()
        # создаем таблицы в БД
        newDB.create_tables()
        # заполняем данными
        fill_data()

# Если в БД данные уже присутствуют
if DBManager.is_exists_data():
    result = input("Использовать текущие данные? (Y/N): ")
    if result.upper() == 'N':
        result_clear = input("Очистить базу и загрузить последние данные? (Y/N): ")
        if result_clear.upper() == 'Y':
            DBManager.clear_data()
            fill_data()


"""   ПРИМЕР   """
# Список компаний с количеством вакансий
print(db_manager.DBManager.get_companies_and_vacancies_count())

# средняя заработная плата
print(db_manager.DBManager.get_avg_salary('RUR'))

# список вакансий с зарплатой выше средней
print(db_manager.DBManager.get_vacancies_with_higher_salary('RUR')[0])

# список выбранных вакансий по критерию
print(db_manager.DBManager.get_vacancies_with_keyword("python")[0])
