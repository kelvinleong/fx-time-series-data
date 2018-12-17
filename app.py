import datetime
import os
import time

import v20
import potsdb
import pytz

HOST = os.environ.get('OTSDB_TEST_HOST', '127.0.0.1')


def create_metric(**kwargs):
    my_kwargs = {"port": 4242, "check_host": False, "test_mode": True}
    my_kwargs.update(kwargs)
    metric = potsdb.client.Client(HOST, port=4242, qsize=0, host_tag=True,
                 mps=0, check_host=True, test_mode=False)
    return metric


def create_context():
    ctx = v20.Context(
        "api-fxpractice.oanda.com",
        443,
        True,
        token=""
    )
    return ctx


def to_utc_datatime(time_str):
    naive_dt = datetime.datetime.strptime(time_str.replace("T", " ").replace("000Z", ""), '%Y-%m-%d %H:%M:%S.%f')
    return pytz.UTC.localize(naive_dt)


def to_epoch_seconds(time_str):
    time_dt = to_utc_datatime(time_str)
    return int(time.mktime(time_dt.timetuple()))


def get_candles():
    instrument = "USD_JPY"
    metric = create_metric()
    start = datetime.datetime(2013, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
    ctx = create_context()

    current = pytz.UTC.localize(datetime.datetime.now() - datetime.timedelta(days = 1))
    while start < current:
        result = ctx.instrument.candles(instrument,
                                        count=5000,
                                        granularity="M1",
                                        fromTime=start.isoformat())

        if result.status != 200:
            print("Failed to fetch candles")
        else:
            candles = result.get("candles")
            for candle in candles:
                metric.send(instrument + "_M1",
                            to_epoch_seconds(candle.time),
                            open=candle.mid.o,
                            high=candle.mid.h,
                            low=candle.mid.l,
                            close=candle.mid.c)

                if candle == candles[-1]:
                    start = to_utc_datatime(candle.time)
                    print(start)

    metric.wait()


def main():
    get_candles()


if __name__ == '__main__':
    main()
