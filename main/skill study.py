from datetime import datetime, timedelta, date, time
from collections import namedtuple
import csv
import sqlite3 as sql
import re
import hashlib
from pathlib import WindowsPath as Path


class LogFile:
    def __init__(self, ud_obj, path):
        """

        :param ud_obj: UserData
        :param path: Path
        """
        self.ud_obj = ud_obj
        self.Path = path
        self.time_part = datetime.min.time()
        self.date_part = datetime.min.date()
        self.datetime_whole = datetime
        self.line_list = list()
        self.start_date = ud_obj.sample_start
        self.log_position = self.get_log_position()
        self.Line = namedtuple('Line', ['time', 'line'])

    def __iter__(self):
        return

    def __next__(self):
        with self.Path.open(encoding='utf-8') as fp:
            fp.seek(self.log_position)
            for line in iter(fp.readline, ''):
                self.log_position = fp.tell()
                line = line.strip()
                if 'Logging' in line[:7]:
                    date1 = datetime.strptime(line[16:].strip(), "%Y-%m-%d").date()
                    if isinstance(date1, datetime):
                        self.date_part = date1.date()
                        continue
                try:
                    time1 = datetime.strptime(line[:10], "[%H:%M:%S]").time()
                except ValueError:
                    pass
                finally:
                    self.time_part = time1
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                if self.datetime_whole > ud.sample_end:
                    break
                self.line_list.append(self.Line(self.datetime_whole, line[11:]))

    def get_log_position(self):
        log_position = list()
        date1 = None
        with self.Path.open(encoding='utf-8') as fp:
            for line in iter(fp.readline, ''):
                log_position.insert(0, fp.tell())
                log_position = log_position[:2]
                line = line.strip()
                if date1 is None or date1 < self.start_date.date():
                    if 'Logging' in line[:7]:
                        date1 = datetime.strptime(line[16:].strip(), "%Y-%m-%d").date()
                        if isinstance(date1, date):
                            self.date_part = date1
                            continue
                if self.date_part >= self.start_date.date():
                    try:
                        time1 = datetime.strptime(line[:10], "[%H:%M:%S]").time()
                    except ValueError:
                        pass
                    finally:
                        self.time_part = time1
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                if self.start_date <= self.datetime_whole:
                    return log_position[1]

    pass


class UserData:
    def __init__(self, file, minute_delta):
        """

        :param file: str
        :param minute_delta: timedelta
        :raise ValueError:
        """
        self.DataTxt = namedtuple('DataTxt', 'label, value')
        for data in map(self.DataTxt._make, csv.reader(open(file, newline=''), quoting=csv.QUOTE_MINIMAL)):
            if data.label == 'event_path' and Path(data.value).exists():
                self.event_path = Path(data.value)
            if data.label == 'skill_path' and Path(data.value).exists():
                self.skill_path = Path(data.value)
            if data.label == 'start_date' and type(eval(data.value)) is datetime:
                self.sample_start = eval(data.value)
            if data.label == 'end_date' and type(eval(data.value)) is datetime:
                self.end_date = eval(data.value)
        # todo Need to add error messages for when a path doesn't exist or a datetime is invalid.
        self.sample_end = self.sample_start + minute_delta
        print('event:{}\nskill:{}\nstart:{}\nend:{}'.format(self.event_path, self.skill_path, self.sample_start,
                                                            self.sample_end))

    def increment_sample_window(self, minuets):
        """

        :param minuets: timedelta
        """
        if type(minuets) is not timedelta:
            # todo need error handling for when minuets is not a timedelta type.
            pass
        self.sample_start += minuets
        self.sample_end += minuets

    pass


class Skill:
    def __init__(self):
        """

        :type self: object
        """
        self.found_times = dict()  # dictionary with values as list objects
        self.sk_values = tuple()
        self.Skill_Values = namedtuple('Skill_Values', ['skill', 'gain', 'level', 'line'])

    def group_same_times(self, log_class, count=0):
        """
        Group skill log entries with same timestamps.

        :param log_class: LogFile
        :param count: int
        """
        if count == 0:
            count = len(log_class.line_list)
        for _ in list(range(count)):
            try:
                line = log_class.line_list.pop(0)
            except IndexError:
                raise StopIteration
            result = re.search('([a-zA-Z\-]+) increased by ([.0-9]+) to ([.0-9]+)', line.line)
            if result:
                self.sk_values = self.Skill_Values(result.group(1), float(result.group(2)), float(result.group(3)),
                                                   line.line)
            if not result:
                raise ValueError
            if line.time in self.found_times:
                # Dictionary with unique datetime keys allows membership tests. Here we can group skill gains by times.
                # Note that dictionary keys aren't ordered so datetime keys will not be chronological order.
                self.found_times[line.time].append(self.sk_values)
            else:
                self.found_times[line.time] = list()
                self.found_times[line.time].append(self.sk_values)

    def remove_older_times(self, oldest_time):
        """
        Remove older grouped entries.

        :param oldest_time: datetime
        """
        keys = tuple(self.found_times.keys())
        for skill_key in keys:
            # print('Old {}, key {}'.format(oldest_time, skill_key))
            if skill_key <= oldest_time:
                del self.found_times[skill_key]
    pass


class Event:
    def __init__(self):
        self.line_matches = list()
        self.sequences = list()
        self.Line = namedtuple('Line', ['time', 'line', 'outcome', 'tool', 'target', 'craft_skill', 'tool_skill',
                                        'action_type'])
        self.Sequence = namedtuple('Sequence', ['start', 'end', 'delta', 'tool', 'target', 'craft_skill', 'tool_skill',
                                                'action_type'])
        self.Match = namedtuple('Match', ['regex', 'capture1', 'capture2', 'capture3', 'capture4',
                                          'outcome', 'tool', 'target', 'craft_skill', 'tool_skill', 'action_type'])
        self.sequence_start = None

    def line_matcher(self, log_class, re_con, count=0):
        """

        :type log_class: LogFile
        :type re_con: _sqlite3.Connection
        :type count: int
        :raise StopIteration:
        """
        if count == 0:
            count = len(log_class.line_list)
        for _ in list(range(count)):
            try:
                line = log_class.line_list.pop(0)
            except IndexError:
                raise StopIteration
            with re_con:
                for _row in re_con.execute('SELECT * FROM REGEX_LOOK'):
                    # Get re patterns from re_con and look for matches in a list passed in from LogFile class.
                    result = re.search(_row[0], line.line)
                    if result:
                        match_len = len(result.groups())
                        match_this = [_row[0]]
                        match_reg = 'SELECT * FROM id_regex where regex=?'
                        construct_reg = list()
                        a = str()
                        for i in range(match_len):
                            match_this.append(result.groups()[i])
                            construct_reg.append(' and capture' + str(i + 1) + '=?')
                        match_reg += a.join(construct_reg)
                        # print('reg: {}\r\nentry: {}'.format(match_reg, match_this))
                        row = None
                        for row in re_con.execute(str(match_reg), tuple(match_this)):
                            pass
                        if row:
                            b = self.Match._make(row)
                            # print("row: {}\r\nline: {}".format(row, _line.line))
                            self.line_matches.append(self.Line(time=line.time, line=line.line, outcome=b.outcome,
                                                               tool=b.tool, target=b.target, craft_skill=b.craft_skill,
                                                               tool_skill=b.tool_skill, action_type=b.action_type))

                        break

    def event_sequencer(self, count=0):
        """

        :type count: int
        :raise StopIteration:
        """
        match_check = ''
        if count == 0:
            count = len(self.line_matches)

        for _ in list(range(count)):
            try:
                line = self.line_matches.pop(0)
            except IndexError:
                raise StopIteration

            if line.outcome == 'start' and self.sequence_start is None:
                self.sequence_start = line
                match_check = '{}_{}_{}_{}_{}'.format(line.tool, line.target, line.craft_skill, line.tool_skill,
                                                      line.action_type)
                continue
            if line.outcome != 'start' and self.sequence_start is None:
                # todo Here is an end without a start, error log is needed here?
                continue
            if line.outcome == 'start' and self.sequence_start is not None:
                # todo Here is a start without an end, error log needed here?
                self.sequence_start = None
                continue
            if line.outcome != 'start' and self.sequence_start is not None:
                if match_check != '{}_{}_{}_{}_{}'.format(self.sequence_start.tool, self.sequence_start.target,
                                                          self.sequence_start.craft_skill,
                                                          self.sequence_start.tool_skill,
                                                          self.sequence_start.action_type):
                    # todo Here is a start and end that don't match, error log needed?
                    pass
                self.sequences.append(self.Sequence(start=self.sequence_start.time, end=line.time,
                                                    delta=line.time - self.sequence_start.time, tool=line.tool,
                                                    target=line.target, craft_skill=line.craft_skill,
                                                    tool_skill=line.tool_skill, action_type=line.action_type))
                self.sequence_start = None
    pass


class SkillEvent:
    def __init__(self):
        self.Match = namedtuple('Match', ['event', 'skill'])
        self.match_list = list()
        self.last_line_time = datetime

    def matcher(self, skill_dict, event_list):
        """

        :type skill_dict: dict
        :type event_list: list
        """
        count = len(event_list)
        for _ in list(range(count)):
            line = event_list.pop(0)
            keys = skill_dict.keys()
            for skill_key in keys:
                if line.end >= skill_key > line.start:
                    self.match_list.append(self.Match(event=line, skill=skill_dict[skill_key]))
            self.last_line_time = line.end
        sk_data.remove_older_times(self.last_line_time)
    pass


def list_pop(my_list):
    try:
        line = my_list.pop(0)
    except IndexError:
        raise StopIteration
    yield line


def regex_setup(sql_arg):
    """

    :param sql_arg: sql.Connection
    :return:
    """
    try:
        sql_arg.execute('''CREATE TABLE regex_look(regex TEXT, outcome TEXT)''')
    except sql.Error:
        pass
    with open("regex.csv", encoding='utf-8', newline='') as fp:
        csv_reader = csv.reader(fp)
        # b'\xe2\x99\xa0' = ♠ for utf-8
        for entries in csv_reader:
            entry_present = sum(len(_rows) for _rows in
                                sql_arg.execute('SELECT * FROM regex_look WHERE regex=? and outcome=?',
                                                (entries[0], entries[1])))
            # Are the entries-csv values in DB?
            if entry_present == 0:
                # Generator will result in 0 if entries-csv values are absent.
                sql_arg.execute('INSERT into regex_look VALUES (?,?)', (entries[0], entries[1]))
    return sql_arg


def id_regex_setup(sql_arg):
    """

    :param sql_arg: sql.Connection
    :return:
    """
    try:
        sql_arg.execute('''CREATE TABLE id_regex(regex TEXT, capture1 TEXT, capture2 TEXT, capture3 TEXT, capture4 TEXT,
                           outcome TEXT, tool TEXT, target TEXT, craft_skill TEXT, tool_skill TEXT,
                           action_type TEXT)''')
    except sql.Error:
        pass
    with open("ID regex.csv", encoding='utf-8', newline='') as fp:
        csv_reader = csv.reader(fp)
        for entries in csv_reader:
            entry_present = sum(len(_rows) for _rows in
                                sql_arg.execute('''SELECT * FROM id_regex WHERE regex=? and capture1=?
                                                and capture2=? and capture3=? and capture4=? and outcome=? and tool=?
                                                and target=? and craft_skill=? and tool_skill=? and action_type=?''',
                                                (entries[0], entries[1], entries[2], entries[3], entries[4],
                                                 entries[5], entries[6], entries[7], entries[8], entries[9],
                                                 entries[10])))
            # Are the entries-csv values in DB?
            if entry_present == 0:
                # Generator will result in 0 if entries-csv values are absent.
                sql_arg.execute('INSERT into id_regex VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                                (entries[0], entries[1], entries[2], entries[3], entries[4], entries[5], entries[6],
                                 entries[7], entries[8], entries[9], entries[10]))
    return sql_arg


def csv_import_generator(file):
    """

    :param file: str
    """
    with open(file) as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        csv_reader = csv.reader(csvfile, dialect)
        for row in csv_reader:
            yield row


def hash_regex(sql_con):
    """

    :param sql_con: sql.connect
    """
    with sql_con:
        for row in sql_con.execute('SELECT regex FROM regex_look'):
            with open('hashed regex.txt', mode='a', encoding='utf-8', newline='') as f:
                f.write('{}\r\n'.format(hashlib.md5(row[0].encode()).hexdigest()))


ud = UserData('my data.txt', timedelta(minutes=15))

sk_log = LogFile(ud, ud.skill_path)
ev_log = LogFile(ud, ud.event_path)
sk_data = Skill()
ev_data = Event()
matched = SkillEvent()

con = sql.connect("regex.sqlite")
con.isolation_level = None

regex_setup(con)
id_regex_setup(con)


sk_log.__next__()
print('skill list: {}'.format(len(sk_log.line_list)))
sk_data.group_same_times(sk_log)
print('skill list: {}'.format(len(sk_log.line_list)))
print('skill key length: {}'.format(len(tuple(sk_data.found_times.keys()))))

ev_log.__next__()
print('line list: {}'.format(len(ev_log.line_list)))
ev_data.line_matcher(ev_log, con)
print('line list: {}'.format(len(ev_log.line_list)))

print('line match: {}'.format(len(ev_data.line_matches)))
ev_data.event_sequencer()
print('line match: {}'.format(len(ev_data.line_matches)))

print('line sequence: {}'.format(len(ev_data.sequences)))
matched.matcher(sk_data.found_times, ev_data.sequences)
print('line sequence: {}'.format(len(ev_data.sequences)))
test1 = len(tuple(sk_data.found_times.keys()))
print('skill key length: {}'.format(test1))
if test1 > 0:
    for key in sk_data.found_times.keys():
        print('key>{}, value>{}'.format(key, sk_data.found_times[key]))
