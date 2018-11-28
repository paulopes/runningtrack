About **runningtrack**
======================

The runningtrack package runs functions in background processes, using a schedule,
and with a timeout/retry capability.

**runningtrack** is a Python package that allows you to launch functions in
background processes, at scheduled weekdays and times, multiple times a
day if necessary, and should a function take longer than a certain
timeout to complete then the function will be terminated and launched
again.

Schedules, timeouts, retries are configurable.

Example
-------

.. code:: python

    import runners


    def main(*argv):
        import runningtrack

        groups = (
            runningtrack.Group("cl1").run(
                # Functions to be launched in parallel:
                runners.pub,
                runners.sub1a,
                runners.sub1b,
                # Everyday at this time:
            ).at("12:30"),

            runningtrack.Group("cl3").run(
                # Functions to be launched in parallel:
                runners.pub,
                runners.sub1a,
                # Every Monday at these two times:
            ).monday("0:45 12:45"),  
        )

        # Use the 'now' parameter to bypass the schedule
        # runnning all runner groups at once and just once.
        if len(argv) > 1 and argv[1].lower() == 'now':
            runningtrack.now(*groups)
        else:
            runningtrack.info()
            runningtrack.wait(*groups)


    if __name__ == "__main__":
        from sys import argv
        main(*argv)


License
-------

Licensed under the Apache License, Version 2.0

*Copyright (c) 2018, Paulo Lopes*