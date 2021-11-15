import datetime
import json

import pyodbc


class Report:
    # обобщенный класс отчёта
    def __init__(self, var_file, et_file, persons_file, terminals_file, departments_file, day, type):
        self.conn_data = self.readvars(var_file)
        self.event_types = self.readvars(et_file)
        self.person_list = self.readvars(persons_file)
        self.terminals = self.readvars(terminals_file)
        self.departments = self.readvars(departments_file)
        self.day = day
        self.type = type
        self.event_list = []

    def readvars(self, var_file):
        # чтение переменных из json файла
        with open(var_file, 'r', encoding="utf8") as fp:
            data = json.load(fp)
        return data

    def connect(self):
        # подключение к базе mssql
        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + self.conn_data["server"] + ';DATABASE='
            + self.conn_data["database"] + ';UID=' + self.conn_data["username"] + ';PWD=' + self.conn_data["password"])
        self.cursor = cnxn.cursor()
        return 0

    def getEvents(self):
        # получение событий из базы и передача функции обработки self.process
        conn_string = "SELECT "
        for key, value in self.event_types.items():
            conn_string += value + ', '
        conn_string = conn_string[:-2] + " FROM " + self.conn_data["tablename"]
        conn_string += " WHERE authDate='" + self.day + "'"
        self.cursor.execute(conn_string)
        self.columnNames = [key for key, value in self.event_types.items()]
        for row in self.cursor.fetchall():
            self.process(row)
        return 0

    def process(self, row):
        # обработка полученных событий и добавление во внутренний список
        self.event_list.append(dict(zip(self.columnNames, row)))
        return 0

    def calculate_attendance(self):
        # подсчет событий посещений (приход, уход)
        self.attendance = {}
        # перебираем всех персонажей
        for key, value in self.person_list.items():
            absent = False
            late_arrival = False
            early_leave = False
            # ищем время первого входа и последнего выхода
            first_in = self.find_first_in(key)
            last_out = self.find_last_out(key)
            # если первого входа не было, значит человек не приходил вовсе
            if first_in == 0:
                absent = True
            else:
                # проверяем на опоздание
                late_arrival = True if first_in > datetime.time(9) else False
                # если человек не выходил, значит и не ушёл раньше
                if last_out == 0:
                    early_leave = False
                # проверяем на ранний уход
                else:
                    early_leave = True if last_out < datetime.time(18) else False
            # записываем это всё в словарь по ключу пользователя
            self.attendance[key] = {'first in': first_in, 'last out': last_out, 'absent': absent,
                                    'late arrival': late_arrival, 'early leave': early_leave}
        return 0

    def find_first_in(self, person_id):
        # поиск первого входа человека
        for event in sorted(self.event_list, key=lambda d: d['time']):
            if event['personid'] == int(person_id) and event['deviceip'] in self.terminals['terminals_in']:
                return event['time']
        return 0

    def find_last_out(self, person_id):
        # поиск последнего выхода человека
        if self.type == 'today':
            return 0
        else:
            for event in sorted(self.event_list, key=lambda d: d['time'], reverse=True):
                if event['personid'] == int(person_id) and event['deviceip'] in self.terminals['terminals_out']:
                    return event['time']
        return 0

    def prepare_report(self):
        self.prepared_report = []
        for key, value in {k: v for k, v in sorted(self.person_list.items(), key=lambda item: item[1]['id']) if
                           v['include'] == 'y'}.items():
            in_message = ''
            out_message = ''
            if value['sex'] == 'm':
                if self.attendance[key]['absent']:
                    in_message = f"{value['id']}"
                elif self.attendance[key]['late arrival']:
                    in_message = f"{value['id']} опоздал на работу в {self.attendance[key]['first in']}."
                else:
                    in_message = f"{value['id']} пришёл на работу в {self.attendance[key]['first in']}."

                if self.attendance[key]['last out'] == 0:
                    pass
                elif self.attendance[key]['early leave']:
                    out_message = f"{value['id']} рано ушёл с работы в {self.attendance[key]['last out']}"
                else:
                    out_message = f"{value['id']} ушёл с работы в {self.attendance[key]['last out']}"
            elif value['sex'] == 'f':
                if self.attendance[key]['absent']:
                    in_message = f"{value['id']}"
                elif self.attendance[key]['late arrival']:
                    in_message = f"{value['id']} опоздала на работу в {self.attendance[key]['first in']}."
                else:
                    in_message = f"{value['id']} пришла на работу в {self.attendance[key]['first in']}."

                if self.attendance[key]['last out'] == 0:
                    pass
                elif self.attendance[key]['early leave']:
                    out_message = f"{value['id']} рано ушла с работы в {self.attendance[key]['last out']}"
                else:
                    out_message = f"{value['id']} ушла с работы в {self.attendance[key]['last out']}"
            self.attendance[key]['message'] = in_message + ' ' + out_message

    def print_report(self):
        rep = []
        rep_struct = {'absent': [],
                      'late': [],
                      'other': {}
                      }
        for department in self.departments['departments']:
            rep_struct['other'][department] = []

        # отсутствующие
        absent_people = []
        for key, value in {k: v for k, v in sorted(self.person_list.items(), key=lambda item: item[1]['id'])
                           if v['include'] == 'y' and self.attendance[k]['absent']}.items():
            absent_people.append(key)
        if len(absent_people) != 0:
            rep.append('Отсутствующие:')
            print('Отсутствующие:')
            for key in absent_people:
                rep.append(self.attendance[key]['message'])
                rep_struct['absent'].append(self.attendance[key]['message'])
                print(self.attendance[key]['message'])

        # опоздавшие
        late_people = []
        for key, value in {k: v for k, v in sorted(self.person_list.items(), key=lambda item: item[1]['id'])
                           if v['include'] == 'y' and self.attendance[k]['late arrival']}.items():
            late_people.append(key)
        if len(late_people) != 0:
            rep.append('Опоздавшие:')
            print('Опоздавшие:')
            for key in late_people:
                rep.append(self.attendance[key]['message'])
                rep_struct['late'].append(self.attendance[key]['message'])
                print(self.attendance[key]['message'])


        for department in self.departments['departments']:
            rep.append(f"Отдел: {department}")
            print(f"Отдел: {department}")
            for key, value in {k: v for k, v in sorted(self.person_list.items(), key=lambda item: item[1]['id'])
                               if v['include'] == 'y'
                                  and v['department'] == department
                                  and not self.attendance[k]['absent']
                                  and not self.attendance[k]['late arrival']}.items():
                rep.append(self.attendance[key]['message'])
                rep_struct['other'][department].append(self.attendance[key]['message'])
                print(self.attendance[key]['message'])
        return rep_struct

    def prepare_person_list(self):
        att_list = {'absent': [],
                      'late': [],
                      'other': {}
                      }
        for department in self.departments['departments']:
            att_list['other'][department] = []

        # отсутствующие
        for key, value in {k: v for k, v in sorted(self.person_list.items(), key=lambda item: item[1]['id'])
                           if v['include'] == 'y' and self.attendance[k]['absent']}.items():
            att_list['absent'].append(key)

        # опоздавшие
        for key, value in {k: v for k, v in sorted(self.person_list.items(), key=lambda item: item[1]['id'])
                           if v['include'] == 'y' and self.attendance[k]['late arrival']}.items():
            att_list['late'].append(key)

        for department in self.departments['departments']:
            for key, value in {k: v for k, v in sorted(self.person_list.items(), key=lambda item: item[1]['id'])
                               if v['include'] == 'y'
                                  and v['department'] == department
                                  and not self.attendance[k]['absent']
                                  and not self.attendance[k]['late arrival']}.items():
                att_list['other'][department].append(key)
        return att_list


if __name__ == "__main__":
    day = datetime.datetime.today().strftime('%Y-%m-%d')
    report = Report('vars.json', 'et.json', 'persons.json', 'terminals.json', 'departments.json', day, 'today')
    report.connect()
    report.getEvents()
    report.calculate_attendance()
    report.prepare_report()
    report.print_report()