
from datetime import datetime
import csv
import re


class SkillLogTimeStamps:
    def __init__(self):
        self.time_list = self.csv_to_skill()
        self.iter_counter = 1
        self.skill_log_path = 'C:/Users/Jason/Documents/wurm/players/joedobo/logs/_Skills.2014-03.txt'

    def __iter__(self):
        return self

    def __next__(self):
        if self.iter_counter == len(self.time_list):
            raise StopIteration
        self.skill_time_stamp = self.time_list[self.iter_counter]
        self.iter_counter += 1

    @staticmethod
    def csv_to_skill():
        """
        Import time stamps.csv into list and convert to datetime objects.
        """
        #print('******** csv_to_skill start ********')
        time_list = list()
        time_list2 = list()
        with open('time stamps.csv') as fp:
            csv_reader = csv.reader(fp, delimiter=',')
            for row in csv_reader:
                time_list = row
        for entry in time_list:
            test = datetime.strptime(entry, '%Y-%m-%d %H:%M:%S')
            time_list2.append(test)
        #print('******** csv_to_skill end ********')
        return time_list2

    def skill_to_csv(self):
        """
        Make a list of skill gain time stamps and write it to time stamps.csv
        """
        time_list = list()
        date_part = None
        time_part = None
        with open(self.skill_log_path, encoding='utf-8') as fp:
            for line in fp:
                line = line.strip()
                result = time_stamp(line)
                if result[0] is not None:
                    date_part = result[0]
                    continue
                if result[1] is not None:
                    time_part = result[1]
                if date_part is None or time_part is None:
                    continue
                datetime_stamp = datetime.combine(date_part, time_part)
                if datetime_stamp not in time_list:
                    time_list.append(datetime_stamp)
                    #print('stamp {}, list length {}'.format(datetime_stamp, len(time_list)))
        with open('time stamps.csv', mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_NONE)
            writer.writerow(time_list)
    pass


class EventLog:
    def __init__(self):
        self.log_pos = [0, 0]
        self.line = str()
        self._date_part = datetime.min.date()
        self._time_part = datetime.min.time()
        self._datetime_stamp = datetime.min
        self.skill_time_stamp = datetime.min
        self.event_log_path = 'C:/Users/Jason/Documents/wurm/players/joedobo/logs/_Event.2014-03.txt'
        self.matched_lines = list()

    def __iter__(self):
        return self

    def __next__(self):
        self.matched_lines = list()
        with open(self.event_log_path, encoding='utf-8') as fp:
            fp.seek(self.log_pos[0])
            for self.line in iter(fp.readline, ''):
                self.log_pos.insert(0, fp.tell())
                self.log_pos = self.log_pos[0:2]
                if len(self.line) <= 2:
                    break
                self.time_stamp()
                if self._date_part.year == 1:
                    continue
                self._datetime_stamp = datetime.combine(self._date_part, self._time_part)
                if self._datetime_stamp < self.skill_time_stamp:
                    continue
                if self._datetime_stamp == self.skill_time_stamp:
                    # need to return a group of same time stamps
                    self.matched_lines.append(self.line.strip())
                    #print(self.line.strip())
                    pass
                if self._datetime_stamp > self.skill_time_stamp:
                    self.log_pos[0] = self.log_pos[1]
                    break

    def time_stamp(self):
        if 'Logging' in self.line[:7]:
            self._date_part = datetime.strptime(self.line[16:].strip(), "%Y-%m-%d").date()
        try:
            self._time_part = datetime.strptime(self.line[:10], "[%H:%M:%S]").time()
        except ValueError:
            pass
    pass


def time_stamp(arg1):
    """
    Convert a string timestamp to datetime object.

    'arg1' is a string containing a time stamp.
    """
    if 'Logging' in arg1[:7]:
        _date_part = datetime.strptime(arg1[16:].strip(), "%Y-%m-%d").date()
        return [_date_part, None]
    try:
        _time_part = datetime.strptime(arg1[:10], "[%H:%M:%S]").time()
    except ValueError:
        return [None, None]
    return [None, _time_part]


def get_regex():
    regex_list = list()
    with open("C:/Users/Jason/Dropbox/PycharmProjects/WOSkillStudy/regex.csv", encoding='utf-8', newline='') as fp:
        csv_reader = csv.reader(fp, quoting=csv.QUOTE_NONE)
        # b'\xe2\x99\xa0' = ♠ for utf-8
        for row in csv_reader:
            regex_list.append(row[0])
        #print(regex_list)
    return regex_list


def main():

    sk = SkillLogTimeStamps()
    ev = EventLog()

    regex_list = get_regex()
    #print(regex_list)

    for _ in sk:
        # iter over each time stamp from sk class.
        #print('i: {}, stamp: {}'.format(sk.iter_counter, sk.skill_time_stamp))
        ev.skill_time_stamp = sk.skill_time_stamp
        next(ev)
        # iter over each event time stamp for a match to the sk class time stamp.
        #print(ev.matched_lines)

        entry_match = False
        for pattern in regex_list:
            for entry in ev.matched_lines:
                #print(entry, entry2)
                result = re.search(pattern, entry)
                if result is not None:
                    entry_match = True
                    break
            if entry_match is True:
                break
        if entry_match is False:
            print('match: {}, stamp: {}'.format(ev.matched_lines, sk.skill_time_stamp))

    if True is False:
        match_list = get_regex()
        with open("C:/Users/Jason/Dropbox/PycharmProjects/WOSkillStudy/test2.txt", encoding='utf-8') as fp:
            for line in fp:
                line_time = line[:10]
                line = line[11:].strip()
                match_found = False
                for regex in match_list:
                    result = re.match(pattern=regex, string=line)
                    if result is None:
                        continue
                    if result is not None:
                        match_found = True
                        #print('regex: {}\r\nstring: {}\r\nresult: {}'.format(regex, line, result))
                        break
                if match_found is False:
                    print(line_time, line)

main()