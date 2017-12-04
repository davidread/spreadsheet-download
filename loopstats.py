import datetime
import time
from collections import defaultdict


class LoopStats(object):
    '''Keep track of the outcomes of iterations a loop. So after processing
    lots of bits of data, you can see how failures there were. Or maybe you
    want to keep track of other sorts of outcomes. It also prints intermediate
    results while its processing, and gives you an idea how quickly it is
    doing the loops.

    Example output:

        12 error parsing date e.g. "Warwick Station"
        639 processed ok e.g. "Cambridge Bus Station"
        Count: 651 Rate: 235,000/hour Time: 0:00:48 taken, 0:04:22 remaining.
        Progress: 16%

    Example usage:

    from loopstats import LoopStats
    loopstats = LoopStats()
    for x in things_to_process:
        try:
            ... process ...
        except Exception as e:
            loopstats.add('error: {}'.format(e), x['id'])
        else:
            loopstats.add('processed ok', x['id'])
        loopstats.print_every_x_iterations(100)
    print(loopstats)
    '''
    def __init__(self, num_iterations=None):
        '''
        num_iterations - the expected number of iterations in the loop.
                         It's required if you want it to show progress % and
                         estimate remaining time.
        '''
        self.start_time = time.time()
        self.outcomes = defaultdict(list)
        self.count = 0
        self.num_iterations = num_iterations

    def add(self, outcome, iteration_id):
        self.outcomes[outcome or 'processed'].append(iteration_id)
        self.count += 1
        return outcome, iteration_id

    def print_every_x_iterations(self, num_iterations, **kwargs):
        if self.count % num_iterations != 0 or self.count == 0:
            return
        print(self.stats(self, **kwargs))

    def __str__(self):
        return self.stats(self)

    def stats(self, print_rate=True):
        time_taken = time.time() - self.start_time
        rate_per_hour = self.count / (time_taken) * 60 * 60
        for outcome, rows in self.outcomes.items():
            print('{:,} {} e.g. {}'.format(len(rows), outcome, rows[0]))
        stats = 'Count: {:,}'.format(self.count)
        if print_rate:
            stats += ' Rate: {:,.0f}/hour'.format(round(rate_per_hour, -3))
        if print_rate:
            stats += ' Time: {} taken' \
                .format(datetime.timedelta(seconds=int(time_taken)))
        if self.num_iterations:
            percent_complete = float(self.count) / self.num_iterations * 100
            time_remaining = time_taken * \
                (self.num_iterations / self.count - 1.0)
            stats += ', {} remaining. Progress: {:.0f}%'.format(
                datetime.timedelta(seconds=int(time_remaining)),
                percent_complete)
        return stats + '\n'
