import time
import json
import requests
from setting import settings


class HeadHunterAPI:
    """ API класс для работы с HH.RU """

    def __init__(self, url: str):
        self.params = {}
        self.url = url
        self.headers = {}

    def get_all(self, pages: int = None, name_block: str = None) -> list:
        """
        Процедура получения всех данных с определенного ресурса
        :param pages: количество страниц для обработки
        :param name_block: наименование блока в котором находится вакансия
        :return: возвращает список словарей с вакансиями
        """
        data = []

        for page in range(0, int(pages)):
            raw = json.loads(self.get_page(page))

            data.extend(raw[str(name_block)])

            if len(data) < 100:
                break

            time.sleep(1)

        return data

    def get_page(self, page: int) -> str:
        """
        Процедура получения одной страницы с определенного ресурса
        :param page: номер страницы
        :return: возвращает список словарей с вакансиями заданной страницы
        """
        data = []
        try:
            self.params["page"] = int(page)
            response = requests.get(self.url + "vacancies", params=self.params, headers=self.headers)
            if response.status_code == 200:
                data = response.content.decode()
                print(".", end='')
                response.close()
        except:
            print(f"[{self.url}]. Запрашиваемая страница [{page}] не найдена")
        finally:
            return data

    def get_areas(self) -> str:
        """
        Процедура получения справочника регионов (страны, области, города)
        :return: список регионов
        """
        raw = []
        try:
            response = requests.get(self.url + "areas")
            if response.status_code == 200:
                data = response.content.decode()
                raw = json.loads(data)
                response.close()
        except:
            print(f"[{self.url}/areas].Запрашиваемая страница не найдена")
        finally:
            return raw

    def get_employer(self, id_employer) -> str:
        """
        Процедура получения данных о работодателе
        """
        raw = []
        try:
            response = requests.get(self.url + "employers/" + str(id_employer))
            if response.status_code == 200:
                data = response.content.decode()
                raw = json.loads(data)
                response.close()
        except:
            print(f"[{self.url}employers/{id_employer}].Запрашиваемая страница не найдена")
        finally:
            return raw

    def get_vacancies(self, employer_id: int) -> list:
        """
        Процедура получения всех вакансий по коду работодателя
        :param employer_id: код работодателя по данным HH.ru
        :return: возвращает список найденных вакансий
        """
        self.params = {'employer_id': employer_id, 'per_page': settings.API_ROW_IN_PAGE}

        data_hh = self.get_all(pages=settings.API_COUNT_PAGES, name_block="items")

        return data_hh
