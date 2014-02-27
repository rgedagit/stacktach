# Copyright (c) 2014 - Rackspace Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to
# deal in the Software without restriction, including without limitation the
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
# IN THE SOFTWARE.

import argparse
import datetime
import json
import math
import sys
import operator
import os

sys.path.append(os.environ.get('STACKTACH_INSTALL_DIR', '/stacktach'))

import usage_audit

from stacktach import datetime_to_decimal as dt
from stacktach import models
from stacktach import stacklog


class AccountManager(object):
    def __init__(self):
        self.tenant_cache = dict()

    def connect(self):
        pass

    def get_tenant_info(self, tenant):
        if tenant not in self.tenant_cache:
             tenant_info = dict(
                     tenant=tenant, 
                     account_type = 'core',
                     billing_type = 'external',
                     account_name = 'unknown account',
                     email = 'anonymous@unknown.com',
                     phone = '1-555-555-1212')
             self.tenant_cache[tenant] = tenant_info
        return self.tenant_cache[tenant]

    def close(self):
        pass


class InstanceHoursReport(object):

    FLAVOR_CLASS_WEIGHTS = dict(standard=1.0)

    def __init__(self, account_manager, time=None, period_length='day'):
        if time is None:
            time = datetime.datetime.utcnow()
        self.start, self.end = usage_audit.get_previous_period(time, period_length)
        self.account_manager = account_manager
        self.flavor_cache = dict()
        self.clear()

    def clear(self):
        self.count = 0 
        self.unit_hours = 0.0
        self.by_tenant_account_type = dict()
        self.by_tenant_billing_type = dict()
        self.by_flavor = dict()
        self.by_flavor_class = dict()
        self.by_account_type = dict()
        self.by_billing_type = dict()

    def _get_verified_exists(self):
        start = dt.dt_to_decimal(self.start)
        end = dt.dt_to_decimal(self.end)
        return models.InstanceExists.objects.filter(
            status=models.InstanceExists.VERIFIED,
            audit_period_beginning__gte=start,
            audit_period_beginning__lte=end,
            audit_period_ending__gte=start,
            audit_period_ending__lte=end)

    def _get_instance_hours(self, exist):
        if (exist.deleted_at is None) or (exist.deleted_at > exist.audit_period_ending):
            end = exist.audit_period_ending
        else:
            end = exist.deleted_at
        if exist.launched_at > exist.audit_period_beginning:
            start = exist.launched_at
        else:
            start = exist.audit_period_beginning
        return math.ceil((end - start)/3600)

    def _get_flavor_info(self, exist):
        flavor = exist.instance_flavor_id
        if flavor not in self.flavor_cache:
            if '-' in flavor:
                flavor_class, n = flavor.split('-', 1)
            else:
                flavor_class = 'standard'
            try:
                payload = json.loads(exist.raw.json)[1]['payload']
            except Exception:
                print "Error loading raw notification data for %s" % exist.id
                raise
            flavor_name = payload['instance_type']
            flavor_size = payload['memory_mb']
            weight = self.FLAVOR_CLASS_WEIGHTS.get(flavor_class, 1.0)
            flavor_units = (flavor_size/256.0) * weight
            self.flavor_cache[flavor] = (flavor, flavor_name, flavor_class, flavor_units)
        return self.flavor_cache[flavor]

    def add_billing_type_hours(self, billing_type, unit_hours):
        if billing_type not in self.by_billing_type:
            self.by_billing_type[billing_type] = dict(count=0, unit_hours=0.0)
        cts = self.by_billing_type[billing_type]
        cts['count'] += 1
        cts['unit_hours'] += unit_hours
        cts['percent_count'] = (float(cts['count'])/self.count) * 100
        cts['percent_unit_hours'] = (cts['unit_hours']/self.unit_hours) * 100

    def add_account_type_hours(self, account_type, unit_hours):
        if account_type not in self.by_account_type:
            self.by_account_type[account_type] = dict(count=0, unit_hours=0.0)
        cts = self.by_account_type[account_type]
        cts['count'] += 1
        cts['unit_hours'] += unit_hours
        cts['percent_count'] = (float(cts['count'])/self.count) * 100
        cts['percent_unit_hours'] = (cts['unit_hours']/self.unit_hours) * 100

    def add_flavor_class_hours(self, flavor_class, unit_hours):
        if flavor_class not in self.by_flavor_class:
            self.by_flavor_class[flavor_class] = dict(count=0, unit_hours=0.0)
        cts = self.by_flavor_class[flavor_class]
        cts['count'] += 1
        cts['unit_hours'] += unit_hours
        cts['percent_count'] = (float(cts['count'])/self.count) * 100
        cts['percent_unit_hours'] = (cts['unit_hours']/self.unit_hours) * 100

    def add_flavor_hours(self, flavor, flavor_name, unit_hours):
        if flavor not in self.by_flavor:
            self.by_flavor[flavor] = dict(count=0, unit_hours=0.0)
        cts = self.by_flavor[flavor]
        cts['count'] += 1
        cts['unit_hours'] += unit_hours
        cts['percent_count'] = (float(cts['count'])/self.count) * 100
        cts['percent_unit_hours'] = (cts['unit_hours']/self.unit_hours) * 100
        cts['flavor_name'] = flavor_name

    def add_tenant_hours(self, tenant_info, unit_hours):
        tenant = tenant_info['tenant']
        account_type = tenant_info['account_type']
        billing_type = tenant_info['billing_type']
        if account_type not in self.by_tenant_account_type:
            self.by_tenant_account_type[account_type] = dict()
        if billing_type not in self.by_tenant_billing_type:
            self.by_tenant_billing_type[billing_type] = dict()
        if tenant not in self.by_tenant_account_type[account_type]:
            cts = dict(count=0, unit_hours=0.0)
            self.by_tenant_account_type[account_type][tenant] = cts
            #if this tenant isn't listed by account_type, it won't be by billing type either.
            self.by_tenant_billing_type[billing_type][tenant] = cts
        cts = self.by_tenant_account_type[account_type][tenant]
        cts['count'] += 1
        cts['unit_hours'] += unit_hours
        cts['percent_count'] = (float(cts['count'])/self.count) * 100
        cts['percent_unit_hours'] = (cts['unit_hours']/self.unit_hours) * 100
        cts.update(tenant_info)

    def compile_hours(self):
        exists = self._get_verified_exists()
        self.count = exists.count()
        self.account_manager.connect()
        for exist in exists:
            hours = self._get_instance_hours(exist)
            flavor, flavor_name, flavor_class, flavor_units = self._get_flavor_info(exist)
            tenant_info = self.account_manager.get_tenant_info(exist.tenant)
            unit_hours = hours * flavor_units
            self.unit_hours += unit_hours
            self.add_flavor_hours(flavor, flavor_name, unit_hours)
            self.add_flavor_class_hours(flavor_class, unit_hours)
            self.add_account_type_hours(tenant_info['account_type'], unit_hours)
            self.add_billing_type_hours(tenant_info['billing_type'], unit_hours)
            self.add_tenant_hours(tenant_info, unit_hours)
        self.account_manager.close()

    def top_hundred(self, key):
        def th(d):
            top = dict()
            for t, customers in d.iteritems():
                top[t] = sorted(customers.values(), key=operator.itemgetter(key), reverse=True)[:100]
            return top
        return dict(account_type=th(self.by_tenant_account_type), billing_type=th(self.by_tenant_billing_type))

    def generate_json(self):
        report = dict(total_instance_count=self.count,
                      total_unit_hours=self.unit_hours,
                      flavor=self.by_flavor,
                      flavor_class=self.by_flavor_class,
                      account_type=self.by_account_type,
                      billing_type=self.by_billing_type,
                      top_hundred_by_count=self.top_hundred('count'),
                      top_hundred_by_unit_hours=self.top_hundred('unit_hours'))
        return json.dumps(report)

    def store(self, json_report):
        report = models.JsonReport(
                    json=json_report,
                    created=dt.dt_to_decimal(datetime.datetime.utcnow()),
                    period_start=self.start,
                    period_end=self.end,
                    version=1,
                    name='instance hours')
        report.save()


def valid_datetime(d):
    try:
        t = datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
        return t
    except Exception, e:
        raise argparse.ArgumentTypeError(
            "'%s' is not in YYYY-MM-DD HH:MM:SS format." % d)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('StackTach Instance Hours Report')
    parser.add_argument('--period_length',
                        choices=['hour', 'day'], default='day')
    parser.add_argument('--utcdatetime',
                        help="Override the end time used to generate report.",
                        type=valid_datetime, default=None)
    parser.add_argument('--store',
                        help="If set to true, report will be stored. "
                             "Otherwise, it will just be printed",
                        type=bool, default=False)
    args = parser.parse_args()

    stacklog.set_default_logger_name('instance_hours')
    parent_logger = stacklog.get_logger('instance_hours', is_parent=True)
    log_listener = stacklog.LogListener(parent_logger)
    log_listener.start()

    account_manager = AccountManager()
    report = InstanceHoursReport(
                account_manager,
                time=args.utcdatetime,
                period_length=args.period_length)

    report.compile_hours()
    json = report.generate_json()

    if not args.store:
        print json
    else:
        report.store(json)
