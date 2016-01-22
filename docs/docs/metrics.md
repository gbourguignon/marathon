---
title: Metrics
---

# Metrics

Marathon currently uses the [Codahale/Dropwizard Metrics](https://github.com/dropwizard/metrics). You can query
the current metrics via the `/metrics` HTTP endpoint or configure reporting these metrics periodically to

* graphite via `--reporter_graphite`.
* datadog via `--reporter_datadog`.
* statsd via `--reporter_datadog` (not a typo, the datadog reports supports statsd).

For the specific syntax check out the
[command line flag metrics]({{ site.baseurl }}/docs/command-line-flags.html#metrics-flags) section.

## Stability of metric names

Even though we try to prevent unnecessary disruptions, we do not provide any stability guarantees for metric
names between major/minor releases. We will not change the name of a metric non-method-call (see below) metric
in a patch release if this is not absolutely required to fix a production issue (very unlikely).

## Metric names

All metric names have to prefixed by the prefix that you configure and are subject to modification by statsd and
the like. For example, if we write that the name of a metric is "service.mesosphere.marathon.uptime", it might
be available under "stats.gauges.marathon_test.service.mesosphere.marathon.uptime" in your configuration.

## Important metrics

`service.mesosphere.marathon.uptime` (gauge) - The uptime of the reporting Marathon process
in milliseconds. This is helpful to diagnose stability problems which cause
Marathon to restart.

### App/group/task counts

`service.mesosphere.marathon.app.count` (gauge) - The number of defined apps. In general,
this is one number which influences the performance of Marathon. If you have
a high number of apps, your performance will be lower than for a low number of
apps.

`service.mesosphere.marathon.group.count` (gauge) - The number of defined groups. In general,
this is one number which influences the performance of Marathon. If you have
a high number of groups, your performance will be lower than for a low number of
groups. Note that each level between the slashes in your app IDs corresponds to
a group. The app `/shop/frontend` is in the `frontend` group which is in
the `shop` group which is in the root group.

<span class="label label-default">v0.15</span>
`service.mesosphere.marathon.task.running.count` (gauge) - The number of tasks that are
currently running.

<span class="label label-default">v0.15</span>
`service.mesosphere.marathon.task.staged.count` (gauge) - The number of tasks that are
currently staged. After tasks are launched, they are first in this state.
A consistently high number of staged tasks indicates a lot of churn in Marathon
and Mesos. Either you have many app updates/manual restarts or some of your apps
have stability problems and are consistently automatically restarted.

### Task update processing

TaskStatusUpdates current, queued

### Configuration update processing

GroupManager current, queued

### ZooKeeper

TBD

### Requests

`org.eclipse.jetty.servlet.ServletContextHandler.dispatches` (timer) - The
number of HTTP requests received by Marathon is available under `.count`.
There are more metrics around HTTP requests under the
`org.eclipse.jetty.servlet.ServletContextHandler` prefix.
For more infos, look at
[the code](https://github.com/dropwizard/metrics/blob/796663609f310888240cc8afb58f75396f8391d2/metrics-jetty9/src/main/java/io/dropwizard/metrics/jetty9/InstrumentedHandler.java#L41-L42).

### JVM

`jvm.threads.count` (meter) - the total number of threads. If this is above >500, this
is generally a bad sign.

`jvm.memory.total.used` (meter) - the total number of bytes used by the Marathon JVM.


## Instrumented method calls

These metrics are created automatically by instrumenting certain classes in our code base which can be disabled
by `--disable_metrics` which will not disable all metrics. Generally, these timers can be very valuable in diagnosing
 unforeseen problems but require detailed insight into the inner workings of Marathon.

Since these metric names directly correspond to class and method names in our code base,
expect the names of these metrics to change if the affected code changes.

## Potential issues

### Derived metrics (mean, p99, ...)

Our metrics library calculates derived metrics like "mean" and "p99". Unfortunately, if reported to statsd, they
do not only relate to the reporting interval but the whole live time of the app with some exponential weighting
algorithm. So try to build your dashboard around "counts" rather than "rates" where possible.

### Statsd, derived statistics and metric names

Statsd typically creates derived statistics (mean, P99) from what is reported. This might interact in a weird
fashion with the derived statistics that our codahale metrics package reports.
