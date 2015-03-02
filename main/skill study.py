from datetime import datetime
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
        self.start_date = ud_obj.start_date
        self.log_position = self.get_log_position()
        self.Line = namedtuple('Line', ['time', 'line'])

    def __iter__(self):
        return

    def __next__(self, _count=50):
        with self.Path.open(encoding='utf-8') as _fp:
            _fp.seek(self.log_position)
            for _line in iter(_fp.readline, ''):
                self.log_position = _fp.tell()
                _line = _line.strip()
                date1, time1 = time_stamp(_line)
                if date1 is not None:
                    self.date_part = date1
                    continue
                if time1 is not None:
                    self.time_part = time1
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                self.line_list.append(self.Line(self.datetime_whole, _line[11:]))
                _count -= 1
                if _count <= 0:
                    break

    def get_log_position(self):
        _log_position = list()
        with self.Path.open(encoding='utf-8') as _fp:
            for _line in iter(_fp.readline, ''):
                _log_position.insert(0, _fp.tell())
                _log_position = _log_position[:2]
                _line = _line.strip()
                date1, time1 = time_stamp(_line)
                if date1 is not None:
                    self.date_part = date1
                    continue
                if time1 is not None:
                    self.time_part = time1
                if self.date_part.year == 1:
                    continue
                self.datetime_whole = datetime.combine(self.date_part, self.time_part)
                if self.start_date <= self.datetime_whole:
                    return _log_position[1]

    pass


class UserData:
    def __init__(self, file):
        """

        :param file: str
        :raise ValueError:
        """
        self.Data = namedtuple('Data', 'skill, event, start, end')
        d = next(map(self.Data._make, csv.reader(open(file))))
        if Path(d.event).exists():
            self.event_path = Path(d.event)
        else:
            raise ValueError
        if Path(d.skill).exists():
            self.skill_path = Path(d.skill)
        else:
            raise ValueError
        if type(eval(d.start)) is datetime:
            self.start_date = eval(d.start)
        else:
            raise ValueError
        if type(eval(d.end)) is datetime:
            self.end_date = eval(d.end)
        else:
            raise ValueError

    pass


class Skill:
    def __init__(self):
        self.found_times = dict()  # dictionary with values as list objects
        self.sk_values = tuple()
        self.Skill_Values = namedtuple('Skill_Values', ['skill', 'gain', 'level', 'line'])

    def group_same_times(self, line_cnt, log_class):
        """

        :param log_class: LogFile
        :param line_cnt: int
        """
        for line in list_pop(log_class.line_list, line_cnt):
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

    def line_matcher(self, line_cnt, log_class, re_con):
        """

        :param line_cnt: int
        :param log_class: LogFile
        :param re_con: sql.connect
        """
        for line in list_pop(log_class.line_list, line_cnt):
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

    def event_sequencer(self, line_count=1):
        """


        :rtype : object
        :param line_count: int
        """
        match_check = ''
        a = list_pop(self.line_matches, line_count)
        for _ in list(range(line_count)):
            try:
                b = a.__next__()
            except StopIteration:
                break

            if b.outcome == 'start' and self.sequence_start is None:
                self.sequence_start = b
                match_check = '{}_{}_{}_{}_{}'.format(b.tool, b.target, b.craft_skill, b.tool_skill, b.action_type)
                continue
            if b.outcome != 'start' and self.sequence_start is None:
                # todo Here is an end without a start, error log is needed here?
                continue
            if b.outcome == 'start' and self.sequence_start is not None:
                # todo Here is a start without an end, error log needed here?
                self.sequence_start = None
                continue
            if b.outcome != 'start' and self.sequence_start is not None:
                if match_check != '{}_{}_{}_{}_{}'.format(self.sequence_start.tool, self.sequence_start.target,
                                                          self.sequence_start.craft_skill,
                                                          self.sequence_start.tool_skill,
                                                          self.sequence_start.action_type):
                    # todo Here is a start and end that don't match, error log needed?
                    pass
                self.sequences.append(self.Sequence(start=self.sequence_start.time, end=b.time,
                                                    delta=b.time - self.sequence_start.time, tool=b.tool,
                                                    target=b.target, craft_skill=b.craft_skill, tool_skill=b.tool_skill,
                                                    action_type=b.action_type))
                self.sequence_start = None
    pass


class SkillEvent:
    def __init__(self):
        self.Match = namedtuple('Match', ['event', 'skill'])
        self.match_list = list()

    def matcher(self, skill_dict, event_list, line_count=1):
        for line in list_pop(event_list, line_count):
            # print(line)
            generator = skill_dict.keys()
            # print('length: {}'.format(len(skill_dict.keys())))
            for skill_key in generator:
                # print('list: {}'.format(skill_dict[skill_key]))
                for gain in list(range(len(skill_dict[skill_key]))):
                    # print('gain: {}'.format(skill_dict[skill_key][gain]))
                    pass
                if line.end >= skill_key > line.start:
                        # print('{}: {}\r\nline: {}'.format(skill_key, skill_dict[skill_key], line))
                        # print('skill: {}, line: {}'.format(skill_key, line.start))
                        # self.match_list.append(self.Match(event=line, skill=skill_dict[skill_key]))
                        pass

    pass


def list_pop(my_list, line_count=1):
    if len(my_list) < line_count:
        line_count = len(my_list)
    for _ in range(line_count):
        try:
            line = my_list.pop(0)
        except IndexError:
            raise StopIteration
        yield line


def time_stamp(arg1):
    """
    Convert a string timestamp to datetime object.

    'arg1' is a string containing a time stamp.
    """
    if 'Logging' in arg1[:7]:
        _date_part = datetime.strptime(arg1[16:].strip(), "%Y-%m-%d").date()
        return _date_part, None
    try:
        _time_part = datetime.strptime(arg1[:10], "[%H:%M:%S]").time()
    except ValueError:
        return None, None
    return None, _time_part


def regex_setup(sql_arg):
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


ud = UserData('my data.txt')

sk_log = LogFile(ud, ud.skill_path)
ev_log = LogFile(ud, ud.event_path)
sk_data = Skill()
ev_data = Event()
matched = SkillEvent()

con = sql.connect("regex.sqlite")
con.isolation_level = None

regex_setup(con)
id_regex_setup(con)

lines_to_do = 500

sk_log.__next__(lines_to_do)
sk_data.group_same_times(1000, sk_log)

ev_log.__next__(lines_to_do)
print('line list: {}'.format(len(ev_log.line_list)))
ev_data.line_matcher(1000, ev_log, con)
print('line list: {}'.format(len(ev_log.line_list)))

print('line match: {}'.format(len(ev_data.line_matches)))
ev_data.event_sequencer(1000)
print('line match: {}'.format(len(ev_data.line_matches)))

print('line sequence: {}'.format(len(ev_data.sequences)))
matched.matcher(sk_data.found_times, ev_data.sequences, 1000)
print('line sequence: {}'.format(len(ev_data.sequences)))

print('match list: {}'.format(len(matched.match_list)))
for entry in matched.match_list:
    print(entry)