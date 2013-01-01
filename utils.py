# -*- coding: utf-8 -*-

import re

class ParseError(Exception):
    pass

class cron_parser(object):
    """ digit   :: '0'..'9'
        number  :: digit+
        range   :: number ( '-' number ) ?
        numspec :: '*' | range
        expr    :: numspec ( '/' number ) ?
        groups  :: expr ( ',' expr ) *
    """

    _range = r'(\d+?)-(\d+)'
    _steps = r'/(\d+)?'
    _star = r'\*'

    def __init__(self, min_=0, max_=60):
        self.min_ = min_
        self.max_ = max_
        self.pats = (
                (re.compile(self._range + self._steps), self._range_steps),
                (re.compile(self._range), self._expand_range),
                (re.compile(self._star + self._steps), self._star_steps),
                (re.compile('^' + self._star + '$'), self._expand_star))

    def parse(self, spec):
        acc = set()
        for part in spec.split(','):
            if not part:
                raise ParseError('empty part')
            acc |= set(self._parse_part(part))
        acc = list(acc)
        acc.sort()
        return acc

    def _parse_part(self, part):
        for regex, handler in self.pats:
            m = regex.match(part)
            if m:
                return handler(m.groups())
        return self._expand_range((part, ))

    def _expand_range(self, toks):
        fr = self._expand_number(toks[0])
        if len(toks) > 1:
            to = self._expand_number(toks[1])
            return range(fr, min(to + 1, self.max_ + 1))
        return [fr]

    def _range_steps(self, toks):
        if len(toks) != 3 or not toks[2]:
            raise ParseError('empty filter')
        return self._expand_range(toks[:2])[::int(toks[2])]

    def _star_steps(self, toks):
        if not toks or not toks[0]:
            raise ParseError('empty filter')
        return self._expand_star()[::int(toks[0])]

    def _expand_star(self, *args):
        return range(self.min_, self.max_ + 1)

    def _expand_number(self, s):
        if isinstance(s, basestring) and s[0] == '-':
            raise ParseError('negative numbers not supported')
        try:
            i = int(s)
        except ValueError:
            raise ValueError("Invalid number '%s'." % s)

        if i < self.min_:
            raise ValueError('Invalid beginning range: %s < %s.' %
                                                   (i, self.min_))
        return i

parse_year = cron_parser(2012, 2050).parse
parse_month = cron_parser(1, 12).parse
parse_day = cron_parser(1, 31).parse
parse_hour = cron_parser(0, 23).parse
parse_minute = cron_parser(0, 59).parse
parse_second = cron_parser(0, 59).parse
parse_array = [parse_year, parse_month, parse_day, parse_hour, parse_minute, parse_second]

if __name__ == '__main__':
    seconds = parse_second('*/4')
    print seconds
    minutes = parse_minute('*/15')
    print minutes
    hours = parse_hour('*/4')
    print hours
    days_of_month = parse_day('*/3')
    print days_of_month
    months_of_year = parse_month('*/2')
    print months_of_year
    years_of_earth = parse_year('*/2')
    print years_of_earth
    years_of_earth = parse_year('2014-2040/2')
    print years_of_earth
