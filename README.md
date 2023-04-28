# parser_apt
1. Создать виртуальное окружение `python3 -m venv /path/to/project`
2. Активация `source venv/bin/activate`
3. Установить зависимости `pip install -r requirements.txt` 
4. Запуск парсера papteki.ru `python main.py papteki`
Результат сохраняется в виде файла с именем `Papteki.xlsx` 
5. Запуск парсера aloeapteka.ru `python main.py aloeapteka`
Конфигурация городов расположено в файле `config.py`
Результат сохраняется в виде файлов c именем `Aloeapteka_{city_name}.xlsx`. 
Количество файлов зависит от количества городов указанных в конфигурации.
6. Запуск парсера 24lek.ru `python main.py lek {city_code} --pharmacies="[{массив названий аптек}]"`. 
Пример: `python main.py lek 0 --pharmacies="['Аптека «Нейрон»', 'Аптека «Аптека СуперФарма»', 'Аптека «Адонис»']"`
Коды доступных городов собраны в файле `lek/config.py`
Результат сохраняется в виде файлов c именем `lek_{city_name}.xlsx`. 
Для работы парсера необходимо заполнить массив названиями препаратов в файле `lek/preparats.json`.
Парсер собирает данный о всех аптеках. В случае повторного выполнения команды пользователю будет предложено 
взять данные из ранее загруженных `Найдены ранее полученные данные. Взять данные из них?(да/нет) по умолчанию (нет):`
