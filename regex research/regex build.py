from datetime import datetime

skill_path = 'C:/Users/Jason/Documents/wurm/players/joedobo/logs/_Skills.2014-03.txt'


class SkillLogTimeStamps:
    def __init__(self, skill_log_path):
        self.iter_counter = 1
        self.skill_log_path = skill_log_path
        self.log_position = 0
        self.time_part = datetime.min.time()
        self.date_part = datetime.min.date()

    def __iter__(self):
        return self

    def __next__(self):
        with open(self.skill_log_path, encoding='utf-8') as fp:
            fp.seek(self.log_position)
            for line in fp:
                line = line.strip()
                result = time_stamp(line)
                if result[0] is not None:
                    self.date_part = result[0]
                    continue
                if result[1] is not None:
                    self.time_part = result[1]
                if self.date_part is None or self.time_part is None:
                    continue
                datetime_stamp = datetime.combine(self.date_part, self.time_part)
            raise StopIteration

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

sk = SkillLogTimeStamps(skill_path)
a = list()
a.insert(0,'abc')