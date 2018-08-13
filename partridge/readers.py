from collections import defaultdict
import datetime

from partridge.config import default_config, reroot_graph
from partridge.gtfs import feed as mkfeed, raw_feed
from partridge.parsers import vparse_date, vparse_time, parse_time
from partridge.utilities import remove_node_attributes


DAY_NAMES = (
    'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
    'saturday', 'sunday')


'''Public'''


def get_filtered_feed(path, filters, config=None):
    '''
    Multi-file feed filtering
    '''
    filter_config = default_config() if config is None else config.copy()
    filter_config = remove_node_attributes(filter_config, 'converters')

    trip_ids = set(raw_feed(path).trips.trip_id)
    for filename, column_filters in filters.items():
        feed = mkfeed(path,
                      config=reroot_graph(filter_config, filename),
                      view={filename: column_filters})
        trip_ids &= set(feed.trips.trip_id)

    return mkfeed(path, config, {'trips.txt': {'trip_id': trip_ids}})


def get_representative_feed(path):
    '''Return a feed filtered to the busiest date'''
    _, service_ids = read_busiest_date(path)
    view = {'trips.txt': {'service_id': service_ids}}
    return mkfeed(path, view=view)


def read_busiest_date(path):
    '''Find the date with the most trips'''
    feed = raw_feed(path)

    service_ids_by_date = _service_ids_by_date(feed)
    trip_counts_by_date = _trip_counts_by_date(feed)

    date, _ = max(trip_counts_by_date.items(), key=lambda p: (p[1], p[0]))
    service_ids = service_ids_by_date[date]

    return date, service_ids


def read_service_ids_by_date(path):
    '''Find all service identifiers by date'''
    feed = raw_feed(path)
    return _service_ids_by_date(feed)


def read_dates_by_service_ids(path):
    '''Find dates with identical service'''
    feed = raw_feed(path)
    return _dates_by_service_ids(feed)


def read_trip_counts_by_date(path):
    '''A useful proxy for busyness'''
    feed = raw_feed(path)
    return _trip_counts_by_date(feed)


def read_trip_ids_by_day(path, cutoff_time):
    '''Find all trip identifiers by day.
    A day is the time between 'date' at cutoff time and 'date'+1
    at cutoff time'''
    feed = raw_feed(path)
    return _trip_ids_by_day(feed, cutoff_time)


'''Private'''


def _service_ids_by_date(feed):
    results = defaultdict(set)
    removals = defaultdict(set)

    service_ids = set(feed.trips.service_id)
    calendar = feed.calendar
    caldates = feed.calendar_dates

    if not calendar.empty:
        # Only consider calendar.txt rows with applicable trips
        calendar = calendar[calendar.service_id.isin(service_ids)].copy()

    if not caldates.empty:
        # Only consider calendar_dates.txt rows with applicable trips
        caldates = caldates[caldates.service_id.isin(service_ids)].copy()

    if not calendar.empty:
        # Parse dates
        calendar.start_date = vparse_date(calendar.start_date)
        calendar.end_date = vparse_date(calendar.end_date)

        # Build up results dict from calendar ranges
        for _, cal in calendar.iterrows():
            start = cal.start_date.toordinal()
            end = cal.end_date.toordinal()

            dow = {i: cal[day] for i, day in enumerate(DAY_NAMES)}
            for ordinal in range(start, end + 1):
                date = datetime.date.fromordinal(ordinal)
                if int(dow[date.weekday()]):
                    results[date].add(cal.service_id)

    if not caldates.empty:
        # Parse dates
        caldates.date = vparse_date(caldates.date)

        # Split out additions and removals
        cdadd = caldates[caldates.exception_type == '1']
        cdrem = caldates[caldates.exception_type == '2']

        # Add to results by date
        for _, cd in cdadd.iterrows():
            results[cd.date].add(cd.service_id)

        # Collect removals
        for _, cd in cdrem.iterrows():
            removals[cd.date].add(cd.service_id)

        # Finally, process removals by date
        for date in removals:
            for service_id in removals[date]:
                if service_id in results[date]:
                    results[date].remove(service_id)

            # Drop the key from results if no service present
            if len(results[date]) == 0:
                del results[date]

    return {k: frozenset(v) for k, v in results.items()}


def _dates_by_service_ids(feed):
    results = defaultdict(set)
    for date, service_ids in _service_ids_by_date(feed).items():
        results[service_ids].add(date)
    return dict(results)


def _trip_counts_by_date(feed):
    results = defaultdict(int)
    trips = feed.trips
    for service_ids, dates in _dates_by_service_ids(feed).items():
        trip_count = trips[trips.service_id.isin(service_ids)].shape[0]
        for date in dates:
            results[date] += trip_count
    return dict(results)


def _keep_first_stop(stop_times):
    """
    Filter 'stop_times' to keep only the first stop for every trip identifier.
    :param stop_times: standard GTFS stop_times table.
    Must contain the columns: 'trip_id', 'stop_sequence', 'arrival_time'.
    :type stop_times: pd.DataFrame
    """
    min_stop = stop_times.groupby('trip_id').stop_sequence.min()
    min_stop_df = min_stop.to_frame().reset_index(drop=False)

    stop_times_f = min_stop_df.merge(stop_times,
                                     on=['trip_id', 'stop_sequence'],
                                     how='inner')
    return stop_times_f


def _trip_ids_by_day(feed, cutoff_time):
    """
    Find all trip identifiers by day, when a day unit is the time between
    'date' at cutoff time and 'date'+1 at cutoff time.
    For example, the key 02-02-2018 with cutoff 16:00 will map to all trip
    identifiers of trips that start between 02-02-2018 16:00
    and 03-02-2018 15:59:59.

    :param feed: ptg.feed object
    :param cutoff_time: HH:MM:SS time string indicating custom day start and
    end time
    """
    cutoff = parse_time(cutoff_time)

    # Get all trip ids with their start time:
    stop_times = feed.stop_times[['trip_id', 'stop_sequence', 'arrival_time']]
    stop_times_f = _keep_first_stop(stop_times)
    stop_times_f.arrival_time = vparse_time(stop_times_f.arrival_time)
    stop_times_f = stop_times_f.set_index('trip_id')
    # stop_times_f will be a df with trip_id as index,
    # and arrival time for the first stop
    stop_times_f = stop_times_f[['arrival_time']]

    # Decide for every trip_id if it starts before or after the time cutoff
    before_cutoff = (stop_times_f.arrival_time < cutoff)

    # Combine dates availability information by service_id
    service_ids_by_date = _service_ids_by_date(feed)
    trips = feed.trips[['service_id', 'trip_id']]
    # for every date, get all relevant trips before and after cutoff:
    tid_by_cutoff = defaultdict(set)
    for date in service_ids_by_date.keys():
        date_service_ids = service_ids_by_date[date]
        for sid in date_service_ids:
            relevant_trip_ids = trips.loc[trips.service_id == sid,
                                          'trip_id'].values

            # All trip ids of this service that start before cutoff:
            tid_before = before_cutoff.loc[relevant_trip_ids]
            tid_by_cutoff[(date, 'before')].update(tid_before[tid_before]
                                                   .index.values)

            # All trip ids of this service that start after cutoff:
            tid_after = ~tid_before
            tid_by_cutoff[(date, 'after')].update(tid_after[tid_after]
                                                  .index.values)

    # for every 'date', merge (date, after) with (date+1, before):
    tid_by_day = defaultdict(set)
    for date in service_ids_by_date.keys():
        next_date = date+datetime.timedelta(days=1)
        tid_by_day[date] = tid_by_cutoff[(date, 'after')]\
            .union(tid_by_cutoff[next_date, 'before'])

    return tid_by_day
