# encoding=UTF8

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Unit tests for the DB API."""
import unittest
import copy
import datetime
import uuid as stdlib_uuid

import iso8601
import mock
import netaddr
from oslo_config import cfg
from oslo_db import api as oslo_db_api
from oslo_db import exception as db_exc
from oslo_db.sqlalchemy import enginefacade
from oslo_db.sqlalchemy import test_base
from oslo_db.sqlalchemy import update_match
from oslo_db.sqlalchemy import utils as sqlalchemyutils
from oslo_serialization import jsonutils
from oslo_utils import fixture as utils_fixture
from oslo_utils import timeutils
from oslo_utils import uuidutils
import six
from six.moves import range
from sqlalchemy import Column
from sqlalchemy.dialects import sqlite
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect
from sqlalchemy import Integer
from sqlalchemy import MetaData
#from sqlalchemy.orm import query
from lib.rome.core.orm.query import Query
from sqlalchemy import sql
from sqlalchemy import Table

from nova import block_device
from nova.compute import arch
from nova.compute import task_states
from nova.compute import vm_states
#from nova import context
from nova.db.discovery import context
from nova import db
from nova.db.discovery import api as discovery_api
from nova.db.discovery import models
from nova.db.sqlalchemy import types as col_types
from nova.db.sqlalchemy import utils as db_utils
from nova import exception
from nova import objects
from nova.objects import fields
from nova import quota
from nova import test
from nova.tests.unit import matchers
from nova.tests import uuidsentinel
from nova import utils

CONF = cfg.CONF
CONF.import_opt('reserved_host_memory_mb', 'nova.compute.resource_tracker')
CONF.import_opt('reserved_host_disk_mb', 'nova.compute.resource_tracker')

test.TestCase.USES_DB = False

def _get_fake_aggr_values():
    return {'name': 'fake_aggregate'}


def _get_fake_aggr_metadata():
    return {'fake_key1': 'fake_value1',
            'fake_key2': 'fake_value2',
            'availability_zone': 'fake_avail_zone'}


def _get_fake_aggr_hosts():
    return ['foo.openstack.org']


def _create_aggregate(context=context.get_admin_context(),
                      values=_get_fake_aggr_values(),
                      metadata=_get_fake_aggr_metadata()):
    return db.aggregate_create(context, values, metadata)


def _create_aggregate_with_hosts(context=context.get_admin_context(),
                      values=_get_fake_aggr_values(),
                      metadata=_get_fake_aggr_metadata(),
                      hosts=_get_fake_aggr_hosts()):
    result = _create_aggregate(context=context,
                               values=values, metadata=metadata)
    for host in hosts:
        db.aggregate_host_add(context, result['id'], host)
    return result

import redis
r =redis.StrictRedis()
class DiscoveryTestCase(test.TestCase):
    def tearDown(self):
        super(DiscoveryTestCase, self).tearDown()
        r.flushall()

class AggregateDBApiTestCase(DiscoveryTestCase):
    def setUp(self):
        super(AggregateDBApiTestCase, self).setUp()
        self.user_id = 'fake'
        self.project_id = 'fake'
        self.context = context.RomeRequestContext(self.user_id, self.project_id)

    def test_aggregate_create_no_metadata(self):
        result = _create_aggregate(metadata=None)
        self.assertEqual(result['name'], 'fake_aggregate')

    def test_aggregate_create_avoid_name_conflict(self):
        r1 = _create_aggregate(metadata=None)
        db.aggregate_delete(context.get_admin_context(), r1['id'])
        values = {'name': r1['name']}
        metadata = {'availability_zone': 'new_zone'}
        r2 = _create_aggregate(values=values, metadata=metadata)
        self.assertEqual(r2['name'], values['name'])
        self.assertEqual(r2['availability_zone'],
                metadata['availability_zone'])

    def test_aggregate_create_raise_exist_exc(self):
        _create_aggregate(metadata=None)
        self.assertRaises(exception.AggregateNameExists,
                          _create_aggregate, metadata=None)

    def test_aggregate_get_raise_not_found(self):
        ctxt = context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_get,
                          ctxt, aggregate_id)

    def test_aggregate_metadata_get_raise_not_found(self):
        ctxt = context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_metadata_get,
                          ctxt, aggregate_id)

    def test_aggregate_create_with_metadata(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        expected_metadata = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(expected_metadata,
                        matchers.DictMatches(_get_fake_aggr_metadata()))

    def test_aggregate_create_delete_create_with_metadata(self):
        # test for bug 1052479
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        expected_metadata = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(expected_metadata,
                        matchers.DictMatches(_get_fake_aggr_metadata()))
        db.aggregate_delete(ctxt, result['id'])
        result = _create_aggregate(metadata={'availability_zone':
            'fake_avail_zone'})
        expected_metadata = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertEqual(expected_metadata, {'availability_zone':
            'fake_avail_zone'})

    def test_aggregate_get(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate_with_hosts(context=ctxt)
        expected = db.aggregate_get(ctxt, result['id'])
        self.assertEqual(_get_fake_aggr_hosts(), expected['hosts'])
        self.assertEqual(_get_fake_aggr_metadata(), expected['metadetails'])

    def test_aggregate_get_by_host(self):
        ctxt = context.get_admin_context()
        values2 = {'name': 'fake_aggregate2'}
        values3 = {'name': 'fake_aggregate3'}
        values4 = {'name': 'fake_aggregate4'}
        values5 = {'name': 'fake_aggregate5'}
        a1 = _create_aggregate_with_hosts(context=ctxt)
        a2 = _create_aggregate_with_hosts(context=ctxt, values=values2)
        # a3 has no hosts and should not be in the results.
        _create_aggregate(context=ctxt, values=values3)
        # a4 has no matching hosts.
        _create_aggregate_with_hosts(context=ctxt, values=values4,
                hosts=['foo4.openstack.org'])
        # a5 has no matching hosts after deleting the only matching host.
        a5 = _create_aggregate_with_hosts(context=ctxt, values=values5,
                hosts=['foo5.openstack.org', 'foo.openstack.org'])
        db.aggregate_host_delete(ctxt, a5['id'],
                                 'foo.openstack.org')
        r1 = db.aggregate_get_by_host(ctxt, 'foo.openstack.org')
        self.assertEqual([a1['id'], a2['id']], [x['id'] for x in r1])

    def test_aggregate_get_by_host_with_key(self):
        ctxt = context.get_admin_context()
        values2 = {'name': 'fake_aggregate2'}
        values3 = {'name': 'fake_aggregate3'}
        values4 = {'name': 'fake_aggregate4'}
        a1 = _create_aggregate_with_hosts(context=ctxt,
                                          metadata={'goodkey': 'good'})
        _create_aggregate_with_hosts(context=ctxt, values=values2)
        _create_aggregate(context=ctxt, values=values3)
        _create_aggregate_with_hosts(context=ctxt, values=values4,
                hosts=['foo4.openstack.org'], metadata={'goodkey': 'bad'})
        # filter result by key
        r1 = db.aggregate_get_by_host(ctxt, 'foo.openstack.org', key='goodkey')
        self.assertEqual([a1['id']], [x['id'] for x in r1])

    def test_aggregate_metadata_get_by_host(self):
        ctxt = context.get_admin_context()
        values = {'name': 'fake_aggregate2'}
        values2 = {'name': 'fake_aggregate3'}
        _create_aggregate_with_hosts(context=ctxt)
        _create_aggregate_with_hosts(context=ctxt, values=values)
        _create_aggregate_with_hosts(context=ctxt, values=values2,
                hosts=['bar.openstack.org'], metadata={'badkey': 'bad'})
        r1 = db.aggregate_metadata_get_by_host(ctxt, 'foo.openstack.org')
        self.assertEqual(r1['fake_key1'], set(['fake_value1']))
        self.assertNotIn('badkey', r1)

    def test_aggregate_metadata_get_by_host_with_key(self):
        ctxt = context.get_admin_context()
        values2 = {'name': 'fake_aggregate12'}
        values3 = {'name': 'fake_aggregate23'}
        a2_hosts = ['foo1.openstack.org', 'foo2.openstack.org']
        a2_metadata = {'good': 'value12', 'bad': 'badvalue12'}
        a3_hosts = ['foo2.openstack.org', 'foo3.openstack.org']
        a3_metadata = {'good': 'value23', 'bad': 'badvalue23'}
        _create_aggregate_with_hosts(context=ctxt)
        _create_aggregate_with_hosts(context=ctxt, values=values2,
                hosts=a2_hosts, metadata=a2_metadata)
        a3 = _create_aggregate_with_hosts(context=ctxt, values=values3,
                hosts=a3_hosts, metadata=a3_metadata)
        r1 = db.aggregate_metadata_get_by_host(ctxt, 'foo2.openstack.org',
                                               key='good')
        self.assertEqual(r1['good'], set(['value12', 'value23']))
        self.assertNotIn('fake_key1', r1)
        self.assertNotIn('bad', r1)
        # Delete metadata
        db.aggregate_metadata_delete(ctxt, a3['id'], 'good')
        r2 = db.aggregate_metadata_get_by_host(ctxt, 'foo3.openstack.org',
                                               key='good')
        self.assertNotIn('good', r2)

    def test_aggregate_get_by_host_not_found(self):
        ctxt = context.get_admin_context()
        _create_aggregate_with_hosts(context=ctxt)
        self.assertEqual([], db.aggregate_get_by_host(ctxt, 'unknown_host'))

    def test_aggregate_delete_raise_not_found(self):
        ctxt = context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_delete,
                          ctxt, aggregate_id)

    def test_aggregate_delete(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)
        db.aggregate_delete(ctxt, result['id'])
        expected = db.aggregate_get_all(ctxt)
        self.assertEqual(0, len(expected))
        aggregate = db.aggregate_get(ctxt.elevated(read_deleted='yes'),
                                     result['id'])
        self.assertEqual(aggregate['deleted'], result['id'])

    def test_aggregate_update(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata={'availability_zone':
            'fake_avail_zone'})
        self.assertEqual(result['availability_zone'], 'fake_avail_zone')
        new_values = _get_fake_aggr_values()
        new_values['availability_zone'] = 'different_avail_zone'
        updated = db.aggregate_update(ctxt, result['id'], new_values)
        self.assertNotEqual(result['availability_zone'],
                            updated['availability_zone'])

    def test_aggregate_update_with_metadata(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)
        values = _get_fake_aggr_values()
        values['metadata'] = _get_fake_aggr_metadata()
        values['availability_zone'] = 'different_avail_zone'
        expected_metadata = copy.deepcopy(values['metadata'])
        expected_metadata['availability_zone'] = values['availability_zone']
        db.aggregate_update(ctxt, result['id'], values)
        metadata = db.aggregate_metadata_get(ctxt, result['id'])
        updated = db.aggregate_get(ctxt, result['id'])
        self.assertThat(metadata,
                        matchers.DictMatches(expected_metadata))
        self.assertNotEqual(result['availability_zone'],
                            updated['availability_zone'])

    def test_aggregate_update_with_existing_metadata(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        values = _get_fake_aggr_values()
        values['metadata'] = _get_fake_aggr_metadata()
        values['metadata']['fake_key1'] = 'foo'
        expected_metadata = copy.deepcopy(values['metadata'])
        db.aggregate_update(ctxt, result['id'], values)
        metadata = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected_metadata))

    def test_aggregate_update_zone_with_existing_metadata(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        new_zone = {'availability_zone': 'fake_avail_zone_2'}
        metadata = _get_fake_aggr_metadata()
        metadata.update(new_zone)
        db.aggregate_update(ctxt, result['id'], new_zone)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_update_raise_not_found(self):
        ctxt = context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        new_values = _get_fake_aggr_values()
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_update, ctxt, aggregate_id, new_values)

    def test_aggregate_update_raise_name_exist(self):
        ctxt = context.get_admin_context()
        _create_aggregate(context=ctxt, values={'name': 'test1'},
                          metadata={'availability_zone': 'fake_avail_zone'})
        _create_aggregate(context=ctxt, values={'name': 'test2'},
                          metadata={'availability_zone': 'fake_avail_zone'})
        aggregate_id = 1
        new_values = {'name': 'test2'}
        self.assertRaises(exception.AggregateNameExists,
                          db.aggregate_update, ctxt, aggregate_id, new_values)

    def test_aggregate_get_all(self):
        ctxt = context.get_admin_context()
        counter = 3
        for c in range(counter):
            _create_aggregate(context=ctxt,
                              values={'name': 'fake_aggregate_%d' % c},
                              metadata=None)
        results = db.aggregate_get_all(ctxt)
        self.assertEqual(len(results), counter)

    def test_aggregate_get_all_non_deleted(self):
        ctxt = context.get_admin_context()
        add_counter = 5
        remove_counter = 2
        aggregates = []
        for c in range(1, add_counter):
            values = {'name': 'fake_aggregate_%d' % c}
            aggregates.append(_create_aggregate(context=ctxt,
                                                values=values, metadata=None))
        for c in range(1, remove_counter):
            db.aggregate_delete(ctxt, aggregates[c - 1]['id'])
        results = db.aggregate_get_all(ctxt)
        self.assertEqual(len(results), add_counter - remove_counter)

    def test_aggregate_metadata_add(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)
        metadata = _get_fake_aggr_metadata()
        db.aggregate_metadata_add(ctxt, result['id'], metadata)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_metadata_add_empty_metadata(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)
        metadata = {}
        db.aggregate_metadata_add(ctxt, result['id'], metadata)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_metadata_add_and_update(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        metadata = _get_fake_aggr_metadata()
        key = list(metadata.keys())[0]
        new_metadata = {key: 'foo',
                        'fake_new_key': 'fake_new_value'}
        metadata.update(new_metadata)
        db.aggregate_metadata_add(ctxt, result['id'], new_metadata)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_metadata_add_retry(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)

        def counted():
            def get_query(context, id, read_deleted):
                get_query.counter += 1
                raise db_exc.DBDuplicateEntry
            get_query.counter = 0
            return get_query

        get_query = counted()
        self.stubs.Set(discovery_api,
                       '_aggregate_metadata_get_query', get_query)
        self.assertRaises(db_exc.DBDuplicateEntry, discovery_api.
                          aggregate_metadata_add, ctxt, result['id'], {},
                          max_retries=5)
        self.assertEqual(get_query.counter, 5)

    def test_aggregate_metadata_update(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        metadata = _get_fake_aggr_metadata()
        key = list(metadata.keys())[0]
        db.aggregate_metadata_delete(ctxt, result['id'], key)
        new_metadata = {key: 'foo'}
        db.aggregate_metadata_add(ctxt, result['id'], new_metadata)
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        metadata[key] = 'foo'
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_metadata_delete(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata=None)
        metadata = _get_fake_aggr_metadata()
        db.aggregate_metadata_add(ctxt, result['id'], metadata)
        db.aggregate_metadata_delete(ctxt, result['id'],
                                     list(metadata.keys())[0])
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        del metadata[list(metadata.keys())[0]]
        self.assertThat(metadata, matchers.DictMatches(expected))

    def test_aggregate_remove_availability_zone(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt, metadata={'availability_zone':
            'fake_avail_zone'})
        db.aggregate_metadata_delete(ctxt, result['id'], 'availability_zone')
        expected = db.aggregate_metadata_get(ctxt, result['id'])
        aggregate = db.aggregate_get(ctxt, result['id'])
        self.assertIsNone(aggregate['availability_zone'])
        self.assertThat({}, matchers.DictMatches(expected))

    def test_aggregate_metadata_delete_raise_not_found(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        self.assertRaises(exception.AggregateMetadataNotFound,
                          db.aggregate_metadata_delete,
                          ctxt, result['id'], 'foo_key')

    def test_aggregate_host_add(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        expected = db.aggregate_host_get_all(ctxt, result['id'])
        self.assertEqual(_get_fake_aggr_hosts(), expected)

    def test_aggregate_host_re_add(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        host = _get_fake_aggr_hosts()[0]
        db.aggregate_host_delete(ctxt, result['id'], host)
        db.aggregate_host_add(ctxt, result['id'], host)
        expected = db.aggregate_host_get_all(ctxt, result['id'])
        self.assertEqual(len(expected), 1)

    def test_aggregate_host_add_duplicate_works(self):
        ctxt = context.get_admin_context()
        r1 = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        r2 = _create_aggregate_with_hosts(ctxt,
                          values={'name': 'fake_aggregate2'},
                          metadata={'availability_zone': 'fake_avail_zone2'})
        h1 = db.aggregate_host_get_all(ctxt, r1['id'])
        h2 = db.aggregate_host_get_all(ctxt, r2['id'])
        self.assertEqual(h1, h2)

    def test_aggregate_host_add_duplicate_raise_exist_exc(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        self.assertRaises(exception.AggregateHostExists,
                          db.aggregate_host_add,
                          ctxt, result['id'], _get_fake_aggr_hosts()[0])

    def test_aggregate_host_add_raise_not_found(self):
        ctxt = context.get_admin_context()
        # this does not exist!
        aggregate_id = 1
        host = _get_fake_aggr_hosts()[0]
        self.assertRaises(exception.AggregateNotFound,
                          db.aggregate_host_add,
                          ctxt, aggregate_id, host)

    def test_aggregate_host_delete(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate_with_hosts(context=ctxt, metadata=None)
        db.aggregate_host_delete(ctxt, result['id'],
                                 _get_fake_aggr_hosts()[0])
        expected = db.aggregate_host_get_all(ctxt, result['id'])
        self.assertEqual(0, len(expected))

    def test_aggregate_host_delete_raise_not_found(self):
        ctxt = context.get_admin_context()
        result = _create_aggregate(context=ctxt)
        self.assertRaises(exception.AggregateHostNotFound,
                          db.aggregate_host_delete,
                          ctxt, result['id'], _get_fake_aggr_hosts()[0])


rome_ignored_keys = ["updated_at", "_rome_version_number", "_metadata_novabase_classname", "_session", "_nova_classname", "_rid"]
import re
class ModelsObjectComparatorMixin(object):
    def _dict_from_object(self, obj, ignored_keys):
        if ignored_keys is None:
            ignored_keys = []
        if isinstance(ignored_keys, six.string_types):
            ignored_keys = [ignored_keys]
        ignored_keys += rome_ignored_keys

        value = {"%s" % str(k): str(v) for k, v in obj.items()
                if "%s" % str(k) not in ignored_keys and not k.startswith("_") and v is not None}
        date_keys = ["created_at", "updated_at", "deleted_at"]
        for date_key in date_keys:
            if date_key in value:
                date_value = value[date_key]
                date_value = date_value.split(".")[0]
                value[date_key] = date_value
        return value

    def _assertEqualObjects(self, obj1, obj2, ignored_keys=None):
        obj1 = self._dict_from_object(obj1, ignored_keys)
        obj2 = self._dict_from_object(obj2, ignored_keys)

        self.assertEqual(len(obj1),
                         len(obj2),
                         "Keys mismatch: %s" %
                          str(set(obj1.keys()) ^ set(obj2.keys())))
        for key, value in obj1.items():
            self.assertEqual(value, obj2[key])

    def _assertEqualListsOfObjects(self, objs1, objs2, ignored_keys=None):
        obj_to_dict = lambda o: self._dict_from_object(o, ignored_keys)
        sort_key = lambda d: [d[k] for k in sorted(d)]
        conv_and_sort = lambda obj: sorted(map(obj_to_dict, obj), key=sort_key)

        self.assertEqual(conv_and_sort(objs1), conv_and_sort(objs2))

    def _assertEqualOrderedListOfObjects(self, objs1, objs2,
                                         ignored_keys=None):
        obj_to_dict = lambda o: self._dict_from_object(o, ignored_keys)
        conv = lambda objs: [obj_to_dict(obj) for obj in objs]

        self.assertEqual(conv(objs1), conv(objs2))

    def _assertEqualListsOfPrimitivesAsSets(self, primitives1, primitives2):
        self.assertEqual(len(primitives1), len(primitives2))
        for primitive in primitives1:
            self.assertIn(primitive, primitives2)

        for primitive in primitives2:
            self.assertIn(primitive, primitives1)


class InstanceTestCase(DiscoveryTestCase, ModelsObjectComparatorMixin):

    """Tests for db.api.instance_* methods."""

    sample_data = {
        'project_id': 'project1',
        'hostname': 'example.com',
        'host': 'h1',
        'node': 'n1',
        'metadata': {'mkey1': 'mval1', 'mkey2': 'mval2'},
        'system_metadata': {'smkey1': 'smval1', 'smkey2': 'smval2'},
        'info_cache': {'ckey': 'cvalue'},
    }

    def setUp(self):
        super(InstanceTestCase, self).setUp()
        self.ctxt = context.get_admin_context()

    def _assertEqualInstances(self, instance1, instance2):
        self._assertEqualObjects(instance1, instance2,
                ignored_keys=['metadata', 'system_metadata', 'info_cache',
                              'extra'])

    def _assertEqualListsOfInstances(self, list1, list2):
        self._assertEqualListsOfObjects(list1, list2,
                ignored_keys=['metadata', 'system_metadata', 'info_cache',
                              'extra'])

    def create_instance_with_args(self, **kwargs):
        if 'context' in kwargs:
            context = kwargs.pop('context')
        else:
            context = self.ctxt
        args = self.sample_data.copy()
        args.update(kwargs)
        return db.instance_create(context, args)

    def test_instance_create(self):
        instance = self.create_instance_with_args()
        self.assertTrue(uuidutils.is_uuid_like(instance['uuid']))

    def test_instance_create_with_object_values(self):
        values = {
            'access_ip_v4': netaddr.IPAddress('1.2.3.4'),
            'access_ip_v6': netaddr.IPAddress('::1'),
            }
        dt_keys = ('created_at', 'deleted_at', 'updated_at',
                   'launched_at', 'terminated_at')
        dt = timeutils.utcnow()
        dt_utc = dt.replace(tzinfo=iso8601.iso8601.Utc())
        for key in dt_keys:
            values[key] = dt_utc
        inst = db.instance_create(self.ctxt, values)
        self.assertEqual(inst['access_ip_v4'], '1.2.3.4')
        self.assertEqual(inst['access_ip_v6'], '::1')
        for key in dt_keys:
            self.assertEqual(inst[key], dt)

    def test_instance_update_with_object_values(self):
        values = {
            'access_ip_v4': netaddr.IPAddress('1.2.3.4'),
            'access_ip_v6': netaddr.IPAddress('::1'),
            }
        dt_keys = ('created_at', 'deleted_at', 'updated_at',
                   'launched_at', 'terminated_at')
        dt = timeutils.utcnow()
        dt_utc = dt.replace(tzinfo=iso8601.iso8601.Utc())
        for key in dt_keys:
            values[key] = dt_utc
        inst = db.instance_create(self.ctxt, {})
        inst = db.instance_update(self.ctxt, inst['uuid'], values)
        self.assertEqual(inst['access_ip_v4'], '1.2.3.4')
        self.assertEqual(inst['access_ip_v6'], '::1')
        for key in dt_keys:
            self.assertEqual(inst[key], dt)

    def test_instance_update_no_metadata_clobber(self):
        meta = {'foo': 'bar'}
        sys_meta = {'sfoo': 'sbar'}
        values = {
            'metadata': meta,
            'system_metadata': sys_meta,
            }
        inst = db.instance_create(self.ctxt, {})
        inst = db.instance_update(self.ctxt, inst['uuid'], values)
        self.assertEqual(meta, utils.metadata_to_dict(inst['metadata']))
        self.assertEqual(sys_meta,
                         utils.metadata_to_dict(inst['system_metadata']))

    def test_instance_get_all_with_meta(self):
        self.create_instance_with_args()
        for inst in db.instance_get_all(self.ctxt):
            meta = utils.metadata_to_dict(inst['metadata'])
            self.assertEqual(meta, self.sample_data['metadata'])
            sys_meta = utils.metadata_to_dict(inst['system_metadata'])
            self.assertEqual(sys_meta, self.sample_data['system_metadata'])

    def test_instance_update(self):
        instance = self.create_instance_with_args()
        metadata = {'host': 'bar', 'key2': 'wuff'}
        system_metadata = {'original_image_ref': 'baz'}
        # Update the metadata
        db.instance_update(self.ctxt, instance['uuid'], {'metadata': metadata,
                           'system_metadata': system_metadata})
        # Retrieve the user-provided metadata to ensure it was successfully
        # updated
        self.assertEqual(metadata,
                db.instance_metadata_get(self.ctxt, instance['uuid']))
        self.assertEqual(system_metadata,
                db.instance_system_metadata_get(self.ctxt, instance['uuid']))

    def test_instance_update_bad_str_dates(self):
        instance = self.create_instance_with_args()
        values = {'created_at': '123'}
        self.assertRaises(ValueError,
                          db.instance_update,
                          self.ctxt, instance['uuid'], values)

    def test_instance_update_good_str_dates(self):
        instance = self.create_instance_with_args()
        values = {'created_at': '2011-01-31T00:00:00.0'}
        actual = db.instance_update(self.ctxt, instance['uuid'], values)
        expected = datetime.datetime(2011, 1, 31)
        self.assertEqual(expected, actual["created_at"])

    # def test_create_instance_unique_hostname(self):
    #     context1 = context.RomeRequestContext('user1', 'p1')
    #     context2 = context.RomeRequestContext('user2', 'p2')
    #     self.create_instance_with_args(hostname='h1', project_id='p1')
    #
    #     # With scope 'global' any duplicate should fail, be it this project:
    #     self.flags(osapi_compute_unique_server_name_scope='global')
    #     self.assertRaises(exception.InstanceExists,
    #                       self.create_instance_with_args,
    #                       context=context1,
    #                       hostname='h1', project_id='p3')
    #     # or another:
    #     self.assertRaises(exception.InstanceExists,
    #                       self.create_instance_with_args,
    #                       context=context2,
    #                       hostname='h1', project_id='p2')
    #     # With scope 'project' a duplicate in the project should fail:
    #     self.flags(osapi_compute_unique_server_name_scope='project')
    #     self.assertRaises(exception.InstanceExists,
    #                       self.create_instance_with_args,
    #                       context=context1,
    #                       hostname='h1', project_id='p1')
    #     # With scope 'project' a duplicate in a different project should work:
    #     self.flags(osapi_compute_unique_server_name_scope='project')
    #     self.create_instance_with_args(context=context2, hostname='h2')
    #     self.flags(osapi_compute_unique_server_name_scope=None)

    def test_instance_get_all_by_filters_empty_list_filter(self):
        filters = {'uuid': []}
        instances = db.instance_get_all_by_filters_sort(self.ctxt, filters)
        self.assertEqual([], instances)

    # @mock.patch('nova.db.sqlalchemy.api.undefer')
    # @mock.patch('nova.db.sqlalchemy.api.joinedload')
    # def test_instance_get_all_by_filters_extra_columns(self,
    #                                                    mock_joinedload,
    #                                                    mock_undefer):
    #     db.instance_get_all_by_filters_sort(
    #         self.ctxt, {},
    #         columns_to_join=['info_cache', 'extra.pci_requests'])
    #     mock_joinedload.assert_called_once_with('info_cache')
    #     mock_undefer.assert_called_once_with('extra.pci_requests')

    # @mock.patch('nova.db.sqlalchemy.api.undefer')
    # @mock.patch('nova.db.sqlalchemy.api.joinedload')
    # def test_instance_get_active_by_window_extra_columns(self,
    #                                                      mock_joinedload,
    #                                                      mock_undefer):
    #     now = datetime.datetime(2013, 10, 10, 17, 16, 37, 156701)
    #     db.instance_get_active_by_window_joined(
    #         self.ctxt, now,
    #         columns_to_join=['info_cache', 'extra.pci_requests'])
    #     mock_joinedload.assert_called_once_with('info_cache')
    #     mock_undefer.assert_called_once_with('extra.pci_requests')

    def test_instance_get_all_by_filters_with_meta(self):
        self.create_instance_with_args()
        for inst in db.instance_get_all_by_filters(self.ctxt, {}):
            meta = utils.metadata_to_dict(inst['metadata'])
            self.assertEqual(meta, self.sample_data['metadata'])
            sys_meta = utils.metadata_to_dict(inst['system_metadata'])
            self.assertEqual(sys_meta, self.sample_data['system_metadata'])

    def test_instance_get_all_by_filters_without_meta(self):
        self.create_instance_with_args()
        result = db.instance_get_all_by_filters(self.ctxt, {},
                                                columns_to_join=[])
        for inst in result:
            meta = utils.metadata_to_dict(inst['metadata'])
            self.assertEqual(meta, {})
            sys_meta = utils.metadata_to_dict(inst['system_metadata'])
            self.assertEqual(sys_meta, {})

    def test_instance_get_all_by_filters(self):
        instances = [self.create_instance_with_args() for i in range(3)]
        filtered_instances = db.instance_get_all_by_filters(self.ctxt, {})
        self._assertEqualListsOfInstances(instances, filtered_instances)

    def test_instance_get_all_by_filters_zero_limit(self):
        self.create_instance_with_args()
        instances = db.instance_get_all_by_filters(self.ctxt, {}, limit=0)
        self.assertEqual([], instances)

    # def test_instance_metadata_get_multi(self):
    #     uuids = [self.create_instance_with_args()['uuid'] for i in range(3)]
    #     #with sqlalchemy_api.main_context_manager.reader.using(self.ctxt):
    #     meta = sqlalchemy_api._instance_metadata_get_multi(
    #         self.ctxt, uuids)
    #     for row in meta:
    #         self.assertIn(row['instance_uuid'], uuids)

    # def test_instance_metadata_get_multi_no_uuids(self):
    #     self.mox.StubOutWithMock(query.Query, 'filter')
    #     self.mox.ReplayAll()
    #     with sqlalchemy_api.main_context_manager.reader.using(self.ctxt):
    #         sqlalchemy_api._instance_metadata_get_multi(self.ctxt, [])

    # def test_instance_system_system_metadata_get_multi(self):
    #     uuids = [self.create_instance_with_args()['uuid'] for i in range(3)]
    #     with sqlalchemy_api.main_context_manager.reader.using(self.ctxt):
    #         sys_meta = sqlalchemy_api._instance_system_metadata_get_multi(
    #         self.ctxt, uuids)
    #     for row in sys_meta:
    #         self.assertIn(row['instance_uuid'], uuids)

    # def test_instance_system_metadata_get_multi_no_uuids(self):
    #     self.mox.StubOutWithMock(query.Query, 'filter')
    #     self.mox.ReplayAll()
    #     sqlalchemy_api._instance_system_metadata_get_multi(self.ctxt, [])

    # def test_instance_get_all_by_filters_regex(self):
    #     i1 = self.create_instance_with_args(display_name='test1')
    #     i2 = self.create_instance_with_args(display_name='teeeest2')
    #     self.create_instance_with_args(display_name='diff')
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'display_name': 't.*st.'})
    #     self._assertEqualListsOfInstances(result, [i1, i2])

    def test_instance_get_all_by_filters_changes_since(self):
        i1 = self.create_instance_with_args(updated_at=
                                            '2013-12-05T15:03:25.000000')
        i2 = self.create_instance_with_args(updated_at=
                                            '2013-12-05T15:03:26.000000')
        changes_since = iso8601.parse_date('2013-12-05T15:03:25.000000')
        result = db.instance_get_all_by_filters(self.ctxt,
                                                {'changes-since':
                                                 changes_since})
        self._assertEqualListsOfInstances([i1, i2], result)

        changes_since = iso8601.parse_date('2013-12-05T15:03:26.000000')
        result = db.instance_get_all_by_filters(self.ctxt,
                                                {'changes-since':
                                                 changes_since})
        self._assertEqualListsOfInstances([i2], result)

        db.instance_destroy(self.ctxt, i1['uuid'])
        filters = {}
        filters['changes-since'] = changes_since
        filters['marker'] = i1['uuid']
        result = db.instance_get_all_by_filters(self.ctxt,
                                                filters)
        self._assertEqualListsOfInstances([i2], result)

    def test_instance_get_all_by_filters_exact_match(self):
        instance = self.create_instance_with_args(host='host1')
        self.create_instance_with_args(host='host12')
        result = db.instance_get_all_by_filters(self.ctxt,
                                                {'host': 'host1'})
        self._assertEqualListsOfInstances([instance], result)

    def test_instance_get_all_by_filters_metadata(self):
        instance = self.create_instance_with_args(metadata={'foo': 'bar'})
        self.create_instance_with_args()
        result = db.instance_get_all_by_filters(self.ctxt,
                                                {'metadata': {'foo': 'bar'}})
        self._assertEqualListsOfInstances([instance], result)

    def test_instance_get_all_by_filters_system_metadata(self):
        instance = self.create_instance_with_args(
                system_metadata={'foo': 'bar'})
        self.create_instance_with_args()
        result = db.instance_get_all_by_filters(self.ctxt,
                {'system_metadata': {'foo': 'bar'}})
        self._assertEqualListsOfInstances([instance], result)
    #
    # def test_instance_get_all_by_filters_unicode_value(self):
    #     i1 = self.create_instance_with_args(display_name=u'test♥')
    #     i2 = self.create_instance_with_args(display_name=u'test')
    #     i3 = self.create_instance_with_args(display_name=u'test♥test')
    #     self.create_instance_with_args(display_name='diff')
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'display_name': u'test'})
    #     self._assertEqualListsOfInstances([i1, i2, i3], result)
    #
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'display_name': u'test♥'})
    #     self._assertEqualListsOfInstances(result, [i1, i3])

    # def test_instance_get_all_by_filters_tags(self):
    #     instance = self.create_instance_with_args(
    #         metadata={'foo': 'bar'})
    #     self.create_instance_with_args()
    #     # For format 'tag-'
    #     result = db.instance_get_all_by_filters(
    #         self.ctxt, {'filter': [
    #             {'name': 'tag-key', 'value': 'foo'},
    #             {'name': 'tag-value', 'value': 'bar'},
    #         ]})
    #     self._assertEqualListsOfInstances([instance], result)
    #     # For format 'tag:'
    #     result = db.instance_get_all_by_filters(
    #         self.ctxt, {'filter': [
    #             {'name': 'tag:foo', 'value': 'bar'},
    #         ]})
    #     self._assertEqualListsOfInstances([instance], result)
    #     # For non-existent tag
    #     result = db.instance_get_all_by_filters(
    #         self.ctxt, {'filter': [
    #             {'name': 'tag:foo', 'value': 'barred'},
    #         ]})
    #     self.assertEqual([], result)
    #
    #     # Confirm with deleted tags
    #     db.instance_metadata_delete(self.ctxt, instance['uuid'], 'foo')
    #     # For format 'tag-'
    #     result = db.instance_get_all_by_filters(
    #         self.ctxt, {'filter': [
    #             {'name': 'tag-key', 'value': 'foo'},
    #         ]})
    #     self.assertEqual([], result)
    #     result = db.instance_get_all_by_filters(
    #         self.ctxt, {'filter': [
    #             {'name': 'tag-value', 'value': 'bar'}
    #         ]})
    #     self.assertEqual([], result)
    #     # For format 'tag:'
    #     result = db.instance_get_all_by_filters(
    #         self.ctxt, {'filter': [
    #             {'name': 'tag:foo', 'value': 'bar'},
    #         ]})
    #     self.assertEqual([], result)

    def test_instance_get_by_uuid(self):
        inst = self.create_instance_with_args()
        result = db.instance_get_by_uuid(self.ctxt, inst['uuid'])
        self._assertEqualInstances(inst, result)

    # Note(msimonin)
    def test_instance_get_by_uuid_dict(self):
        inst = self.create_instance_with_args()
        result = db.instance_get_by_uuid(self.ctxt, inst['uuid'], columns_to_join=['extra'])
        self.assertTrue(hasattr(result, '__dict__'))
        self.assertTrue('extra' in result.__dict__)


    # def test_instance_get_by_uuid_join_empty(self):
    #     inst = self.create_instance_with_args()
    #     result = db.instance_get_by_uuid(self.ctxt, inst['uuid'],
    #             columns_to_join=[])
    #     meta = utils.metadata_to_dict(result['metadata'])
    #     self.assertEqual(meta, {})
    #     sys_meta = utils.metadata_to_dict(result['system_metadata'])
    #     self.assertEqual(sys_meta, {})

    # def test_instance_get_by_uuid_join_meta(self):
    #     inst = self.create_instance_with_args()
    #     result = db.instance_get_by_uuid(self.ctxt, inst['uuid'],
    #                 columns_to_join=['metadata'])
    #     meta = utils.metadata_to_dict(result['metadata'])
    #     self.assertEqual(meta, self.sample_data['metadata'])
    #     sys_meta = utils.metadata_to_dict(result['system_metadata'])
    #     self.assertEqual(sys_meta, {})

    # def test_instance_get_by_uuid_join_sys_meta(self):
    #     inst = self.create_instance_with_args()
    #     result = db.instance_get_by_uuid(self.ctxt, inst['uuid'],
    #             columns_to_join=['system_metadata'])
    #     meta = utils.metadata_to_dict(result['metadata'])
    #     self.assertEqual(meta, {})
    #     sys_meta = utils.metadata_to_dict(result['system_metadata'])
    #     self.assertEqual(sys_meta, self.sample_data['system_metadata'])

    def test_instance_get_all_by_filters_deleted(self):
        inst1 = self.create_instance_with_args()
        inst2 = self.create_instance_with_args(reservation_id='b')
        db.instance_destroy(self.ctxt, inst1['uuid'])
        result = db.instance_get_all_by_filters(self.ctxt, {})
        self._assertEqualListsOfObjects([inst1, inst2], result,
            ignored_keys=['metadata', 'system_metadata',
                          'deleted', 'deleted_at', 'info_cache',
                          'pci_devices', 'extra'])

    # def test_instance_get_all_by_filters_deleted_and_soft_deleted(self):
    #     inst1 = self.create_instance_with_args()
    #     inst2 = self.create_instance_with_args(vm_state=vm_states.SOFT_DELETED)
    #     self.create_instance_with_args()
    #     db.instance_destroy(self.ctxt, inst1['uuid'])
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'deleted': True})
    #     self._assertEqualListsOfObjects([inst1, inst2], result,
    #         ignored_keys=['metadata', 'system_metadata',
    #                       'deleted', 'deleted_at', 'info_cache',
    #                       'pci_devices', 'extra'])

    # def test_instance_get_all_by_filters_deleted_no_soft_deleted(self):
    #     inst1 = self.create_instance_with_args()
    #     self.create_instance_with_args(vm_state=vm_states.SOFT_DELETED)
    #     self.create_instance_with_args()
    #     db.instance_destroy(self.ctxt, inst1['uuid'])
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'deleted': True,
    #                                              'soft_deleted': False})
    #     self._assertEqualListsOfObjects([inst1], result,
    #             ignored_keys=['deleted', 'deleted_at', 'metadata',
    #                           'system_metadata', 'info_cache', 'pci_devices',
    #                           'extra'])

    def test_instance_get_all_by_filters_alive_and_soft_deleted(self):
        inst1 = self.create_instance_with_args()
        inst2 = self.create_instance_with_args(vm_state=vm_states.SOFT_DELETED)
        inst3 = self.create_instance_with_args()
        db.instance_destroy(self.ctxt, inst1['uuid'])
        result = db.instance_get_all_by_filters(self.ctxt,
                                                {'deleted': False,
                                                 'soft_deleted': True})
        self._assertEqualListsOfInstances([inst2, inst3], result)

    def test_instance_get_all_by_filters_not_deleted(self):
        inst1 = self.create_instance_with_args()
        self.create_instance_with_args(vm_state=vm_states.SOFT_DELETED)
        inst3 = self.create_instance_with_args()
        inst4 = self.create_instance_with_args(vm_state=vm_states.ACTIVE)
        db.instance_destroy(self.ctxt, inst1['uuid'])
        result = db.instance_get_all_by_filters(self.ctxt,
                                                {'deleted': False})
        self.assertIsNone(inst3.vm_state)
        self._assertEqualListsOfInstances([inst3, inst4], result)

    def test_instance_get_all_by_filters_cleaned(self):
        inst1 = self.create_instance_with_args()
        inst2 = self.create_instance_with_args(reservation_id='b')
        db.instance_update(self.ctxt, inst1['uuid'], {'cleaned': 1})
        result = db.instance_get_all_by_filters(self.ctxt, {})
        self.assertEqual(2, len(result))
        self.assertIn(inst1['uuid'], [result[0]['uuid'], result[1]['uuid']])
        self.assertIn(inst2['uuid'], [result[0]['uuid'], result[1]['uuid']])
        if inst1['uuid'] == result[0]['uuid']:
            self.assertTrue(result[0]['cleaned'])
            self.assertFalse(result[1]['cleaned'])
        else:
            self.assertTrue(result[1]['cleaned'])
            self.assertFalse(result[0]['cleaned'])

    # def test_instance_get_all_by_filters_tag_any(self):
    #     inst1 = self.create_instance_with_args()
    #     inst2 = self.create_instance_with_args()
    #     inst3 = self.create_instance_with_args()
    #
    #     t1 = u'tag1'
    #     t2 = u'tag2'
    #     t3 = u'tag3'
    #
    #     db.instance_tag_set(self.ctxt, inst1.uuid, [t1])
    #     db.instance_tag_set(self.ctxt, inst2.uuid, [t1, t2, t3])
    #     db.instance_tag_set(self.ctxt, inst3.uuid, [t3])
    #
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'tags-any': [t1, t2]})
    #     self._assertEqualListsOfObjects([inst1, inst2], result,
    #             ignored_keys=['deleted', 'deleted_at', 'metadata', 'extra',
    #                           'system_metadata', 'info_cache', 'pci_devices'])

    # def test_instance_get_all_by_filters_tag_any_empty(self):
    #     inst1 = self.create_instance_with_args()
    #     inst2 = self.create_instance_with_args()
    #
    #     t1 = u'tag1'
    #     t2 = u'tag2'
    #     t3 = u'tag3'
    #     t4 = u'tag4'
    #
    #     db.instance_tag_set(self.ctxt, inst1.uuid, [t1])
    #     db.instance_tag_set(self.ctxt, inst2.uuid, [t1, t2])
    #
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'tags-any': [t3, t4]})
    #     self.assertEqual([], result)

    # def test_instance_get_all_by_filters_tag(self):
    #     inst1 = self.create_instance_with_args()
    #     inst2 = self.create_instance_with_args()
    #     inst3 = self.create_instance_with_args()
    #
    #     t1 = u'tag1'
    #     t2 = u'tag2'
    #     t3 = u'tag3'
    #
    #     db.instance_tag_set(self.ctxt, inst1.uuid, [t1, t3])
    #     db.instance_tag_set(self.ctxt, inst2.uuid, [t1, t2])
    #     db.instance_tag_set(self.ctxt, inst3.uuid, [t1, t2, t3])
    #
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'tags': [t1, t2]})
    #     self._assertEqualListsOfObjects([inst2, inst3], result,
    #             ignored_keys=['deleted', 'deleted_at', 'metadata', 'extra',
    #                           'system_metadata', 'info_cache', 'pci_devices'])
    #
    # def test_instance_get_all_by_filters_tag_empty(self):
    #     inst1 = self.create_instance_with_args()
    #     inst2 = self.create_instance_with_args()
    #
    #     t1 = u'tag1'
    #     t2 = u'tag2'
    #     t3 = u'tag3'
    #
    #     db.instance_tag_set(self.ctxt, inst1.uuid, [t1])
    #     db.instance_tag_set(self.ctxt, inst2.uuid, [t1, t2])
    #
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'tags': [t3]})
    #     self.assertEqual([], result)
    #
    # def test_instance_get_all_by_filters_tag_any_and_tag(self):
    #     inst1 = self.create_instance_with_args()
    #     inst2 = self.create_instance_with_args()
    #     inst3 = self.create_instance_with_args()
    #
    #     t1 = u'tag1'
    #     t2 = u'tag2'
    #     t3 = u'tag3'
    #     t4 = u'tag4'
    #
    #     db.instance_tag_set(self.ctxt, inst1.uuid, [t1, t2])
    #     db.instance_tag_set(self.ctxt, inst2.uuid, [t1, t2, t4])
    #     db.instance_tag_set(self.ctxt, inst3.uuid, [t2, t3])
    #
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'tags': [t1, t2],
    #                                              'tags-any': [t3, t4]})
    #     self._assertEqualListsOfObjects([inst2], result,
    #             ignored_keys=['deleted', 'deleted_at', 'metadata', 'extra',
    #                           'system_metadata', 'info_cache', 'pci_devices'])
    #
    # def test_instance_get_all_by_filters_tags_and_project_id(self):
    #     context1 = context.RomeRequestContext('user1', 'p1')
    #     context2 = context.RomeRequestContext('user2', 'p2')
    #
    #     inst1 = self.create_instance_with_args(context=context1,
    #                                            project_id='p1')
    #     inst2 = self.create_instance_with_args(context=context1,
    #                                            project_id='p1')
    #     inst3 = self.create_instance_with_args(context=context2,
    #                                            project_id='p2')
    #     t1 = u'tag1'
    #     t2 = u'tag2'
    #     t3 = u'tag3'
    #     t4 = u'tag4'
    #
    #     db.instance_tag_set(context1, inst1.uuid, [t1, t2])
    #     db.instance_tag_set(context1, inst2.uuid, [t1, t2, t4])
    #     db.instance_tag_set(context2, inst3.uuid, [t1, t2, t3, t4])
    #
    #     result = db.instance_get_all_by_filters(self.ctxt,
    #                                             {'tags': [t1, t2],
    #                                              'tags-any': [t3, t4],
    #                                              'project_id': 'p1'})
    #     self._assertEqualListsOfObjects([inst2], result,
    #             ignored_keys=['deleted', 'deleted_at', 'metadata', 'extra',
    #                           'system_metadata', 'info_cache', 'pci_devices'])

    def test_instance_get_all_by_host_and_node_no_join(self):
        instance = self.create_instance_with_args()
        result = db.instance_get_all_by_host_and_node(self.ctxt, 'h1', 'n1')
        self.assertEqual(result[0]['uuid'], instance['uuid'])
        self.assertEqual(result[0]['system_metadata'], [])

    def test_instance_get_all_by_host_and_node(self):
        instance = self.create_instance_with_args(
            system_metadata={'foo': 'bar'})
        result = db.instance_get_all_by_host_and_node(
            self.ctxt, 'h1', 'n1',
            columns_to_join=['system_metadata', 'extra'])
        self.assertEqual(instance['uuid'], result[0]['uuid'])
        self.assertEqual('bar', result[0]['system_metadata'][0]['value'])
        self.assertEqual(instance['uuid'], result[0]['extra']['instance_uuid'])

    @mock.patch('nova.db.discovery.api._instances_fill_metadata')
    @mock.patch('nova.db.discovery.api._instance_get_all_query')
    def test_instance_get_all_by_host_and_node_fills_manually(self,
                                                              mock_getall,
                                                              mock_fill):
        db.instance_get_all_by_host_and_node(
            self.ctxt, 'h1', 'n1',
            columns_to_join=['metadata', 'system_metadata', 'extra', 'foo'])
        self.assertEqual(sorted(['extra', 'foo']),
                         sorted(mock_getall.call_args[1]['joins']))
        self.assertEqual(sorted(['metadata', 'system_metadata']),
                         sorted(mock_fill.call_args[1]['manual_joins']))

    def _get_base_values(self):
        return {
            'name': 'fake_sec_group',
            'description': 'fake_sec_group_descr',
            'user_id': 'fake',
            'project_id': 'fake',
            'instances': []
            }

    def _get_base_rule_values(self):
        return {
            'protocol': "tcp",
            'from_port': 80,
            'to_port': 8080,
            'cidr': None,
            'deleted': 0,
            'deleted_at': None,
            'grantee_group': None,
            'updated_at': None
            }

    def _create_security_group(self, values):
        v = self._get_base_values()
        v.update(values)
        return db.security_group_create(self.ctxt, v)

    def _create_security_group_rule(self, values):
        v = self._get_base_rule_values()
        v.update(values)
        return db.security_group_rule_create(self.ctxt, v)

    def test_instance_get_all_by_grantee_security_groups(self):
        instance1 = self.create_instance_with_args()
        instance2 = self.create_instance_with_args()
        instance3 = self.create_instance_with_args()
        secgroup1 = self._create_security_group(
            {'name': 'fake-secgroup1', 'instances': [instance1]})
        secgroup2 = self._create_security_group(
            {'name': 'fake-secgroup2', 'instances': [instance1]})
        secgroup3 = self._create_security_group(
            {'name': 'fake-secgroup3', 'instances': [instance2]})
        secgroup4 = self._create_security_group(
            {'name': 'fake-secgroup4', 'instances': [instance2, instance3]})
        self._create_security_group_rule({'grantee_group': secgroup1,
                                          'parent_group': secgroup3})
        self._create_security_group_rule({'grantee_group': secgroup2,
                                          'parent_group': secgroup4})
        group_ids = [secgroup['id'] for secgroup in [secgroup1, secgroup2]]
        instances = db.instance_get_all_by_grantee_security_groups(self.ctxt,
                                                                   group_ids)
        instance_uuids = [instance['uuid'] for instance in instances]
        self.assertEqual(len(instances), 2)
        self.assertIn(instance2['uuid'], instance_uuids)
        self.assertIn(instance3['uuid'], instance_uuids)

    def test_instance_get_all_by_grantee_security_groups_empty_group_ids(self):
        results = db.instance_get_all_by_grantee_security_groups(self.ctxt, [])
        self.assertEqual([], results)

    def test_instance_get_all_hung_in_rebooting(self):
        # Ensure no instances are returned.
        results = db.instance_get_all_hung_in_rebooting(self.ctxt, 10)
        self.assertEqual([], results)

        # Ensure one rebooting instance with updated_at older than 10 seconds
        # is returned.
        instance = self.create_instance_with_args(task_state="rebooting",
                updated_at=datetime.datetime(2000, 1, 1, 12, 0, 0))
        results = db.instance_get_all_hung_in_rebooting(self.ctxt, 10)
        self._assertEqualListsOfObjects([instance], results,
            ignored_keys=['task_state', 'info_cache', 'security_groups',
                          'metadata', 'system_metadata', 'pci_devices',
                          'extra'])
        db.instance_update(self.ctxt, instance['uuid'], {"task_state": None})

        # Ensure the newly rebooted instance is not returned.
        self.create_instance_with_args(task_state="rebooting",
                                       updated_at=timeutils.utcnow())
        results = db.instance_get_all_hung_in_rebooting(self.ctxt, 10)
        self.assertEqual([], results)

    def test_instance_update_with_expected_vm_state(self):
        instance = self.create_instance_with_args(vm_state='foo')
        db.instance_update(self.ctxt, instance['uuid'], {'host': 'h1',
                                       'expected_vm_state': ('foo', 'bar')})

    def test_instance_update_with_unexpected_vm_state(self):
        instance = self.create_instance_with_args(vm_state='foo')
        self.assertRaises(exception.InstanceUpdateConflict,
                    db.instance_update, self.ctxt, instance['uuid'],
                    {'host': 'h1', 'expected_vm_state': ('spam', 'bar')})

    def test_instance_update_with_instance_uuid(self):
        # test instance_update() works when an instance UUID is passed.
        ctxt = context.get_admin_context()

        # Create an instance with some metadata
        values = {'metadata': {'host': 'foo', 'key1': 'meow'},
                  'system_metadata': {'original_image_ref': 'blah'}}
        instance = db.instance_create(ctxt, values)

        # Update the metadata
        values = {'metadata': {'host': 'bar', 'key2': 'wuff'},
                  'system_metadata': {'original_image_ref': 'baz'}}
        db.instance_update(ctxt, instance['uuid'], values)

        # Retrieve the user-provided metadata to ensure it was successfully
        # updated
        instance_meta = db.instance_metadata_get(ctxt, instance['uuid'])
        self.assertEqual('bar', instance_meta['host'])
        self.assertEqual('wuff', instance_meta['key2'])
        self.assertNotIn('key1', instance_meta)

        # Retrieve the system metadata to ensure it was successfully updated
        system_meta = db.instance_system_metadata_get(ctxt, instance['uuid'])
        self.assertEqual('baz', system_meta['original_image_ref'])

    def test_delete_instance_metadata_on_instance_destroy(self):
        ctxt = context.get_admin_context()
        # Create an instance with some metadata
        values = {'metadata': {'host': 'foo', 'key1': 'meow'},
                  'system_metadata': {'original_image_ref': 'blah'}}
        instance = db.instance_create(ctxt, values)
        instance_meta = db.instance_metadata_get(ctxt, instance['uuid'])
        self.assertEqual('foo', instance_meta['host'])
        self.assertEqual('meow', instance_meta['key1'])
        db.instance_destroy(ctxt, instance['uuid'])
        instance_meta = db.instance_metadata_get(ctxt, instance['uuid'])
        # Make sure instance metadata is deleted as well
        self.assertEqual({}, instance_meta)

    def test_delete_instance_faults_on_instance_destroy(self):
        ctxt = context.get_admin_context()
        uuid = str(stdlib_uuid.uuid4())
        # Create faults
        db.instance_create(ctxt, {'uuid': uuid})

        fault_values = {
            'message': 'message',
            'details': 'detail',
            'instance_uuid': uuid,
            'code': 404,
            'host': 'localhost'
        }
        fault = db.instance_fault_create(ctxt, fault_values)

        # Retrieve the fault to ensure it was successfully added
        faults = db.instance_fault_get_by_instance_uuids(ctxt, [uuid])
        self.assertEqual(1, len(faults[uuid]))
        self._assertEqualObjects(fault, faults[uuid][0])
        db.instance_destroy(ctxt, uuid)
        faults = db.instance_fault_get_by_instance_uuids(ctxt, [uuid])
        # Make sure instance faults is deleted as well
        self.assertEqual(0, len(faults[uuid]))

    def test_instance_update_and_get_original(self):
        instance = self.create_instance_with_args(vm_state='building')
        (old_ref, new_ref) = db.instance_update_and_get_original(self.ctxt,
                            instance['uuid'], {'vm_state': 'needscoffee'})
        self.assertEqual('building', old_ref['vm_state'])
        self.assertEqual('needscoffee', new_ref['vm_state'])

    def test_instance_update_and_get_original_metadata(self):
        instance = self.create_instance_with_args()
        columns_to_join = ['metadata']
        (old_ref, new_ref) = db.instance_update_and_get_original(
            self.ctxt, instance['uuid'], {'vm_state': 'needscoffee'},
            columns_to_join=columns_to_join)
        meta = utils.metadata_to_dict(new_ref['metadata'])
        self.assertEqual(meta, self.sample_data['metadata'])
        sys_meta = utils.metadata_to_dict(new_ref['system_metadata'])
        self.assertEqual(sys_meta, {})

    def test_instance_update_and_get_original_metadata_none_join(self):
        instance = self.create_instance_with_args()
        (old_ref, new_ref) = db.instance_update_and_get_original(
            self.ctxt, instance['uuid'], {'metadata': {'mk1': 'mv3'}})
        meta = utils.metadata_to_dict(new_ref['metadata'])
        self.assertEqual(meta, {'mk1': 'mv3'})

    # def test_instance_update_and_get_original_no_conflict_on_session(self):
    #     with sqlalchemy_api.main_context_manager.writer.using(self.ctxt):
    #         instance = self.create_instance_with_args()
    #         (old_ref, new_ref) = db.instance_update_and_get_original(
    #             self.ctxt, instance['uuid'], {'metadata': {'mk1': 'mv3'}})
    #
    #         # test some regular persisted fields
    #         self.assertEqual(old_ref.uuid, new_ref.uuid)
    #         self.assertEqual(old_ref.project_id, new_ref.project_id)
    #
    #         # after a copy operation, we can assert:
    #
    #         # 1. the two states have their own InstanceState
    #         old_insp = inspect(old_ref)
    #         new_insp = inspect(new_ref)
    #         self.assertNotEqual(old_insp, new_insp)
    #
    #         # 2. only one of the objects is still in our Session
    #         self.assertIs(new_insp.session, self.ctxt.session)
    #         self.assertIsNone(old_insp.session)
    #
    #         # 3. The "new" object remains persistent and ready
    #         # for updates
    #         self.assertTrue(new_insp.persistent)
    #
    #         # 4. the "old" object is detached from this Session.
    #         self.assertTrue(old_insp.detached)

    def test_instance_update_and_get_original_conflict_race(self):
        # Ensure that we retry if update_on_match fails for no discernable
        # reason
        instance = self.create_instance_with_args()

        orig_update_on_match = update_match.update_on_match

        # Reproduce the conditions of a race between fetching and updating the
        # instance by making update_on_match fail for no discernable reason the
        # first time it is called, but work normally the second time.
        with mock.patch.object(update_match, 'update_on_match',
                        side_effect=[update_match.NoRowsMatched,
                                     orig_update_on_match]):
            db.instance_update_and_get_original(
                self.ctxt, instance['uuid'], {'metadata': {'mk1': 'mv3'}})
            self.assertEqual(update_match.update_on_match.call_count, 2)

    # def test_instance_update_and_get_original_conflict_race_fallthrough(self):
    #     # Ensure that is update_match continuously fails for no discernable
    #     # reason, we evantually raise UnknownInstanceUpdateConflict
    #     instance = self.create_instance_with_args()
    #
    #     # Reproduce the conditions of a race between fetching and updating the
    #     # instance by making update_on_match fail for no discernable reason.
    #     with mock.patch.object(update_match, 'update_on_match',
    #                     side_effect=update_match.NoRowsMatched):
    #         self.assertRaises(exception.UnknownInstanceUpdateConflict,
    #                           db.instance_update_and_get_original,
    #                           self.ctxt,
    #                           instance['uuid'],
    #                           {'metadata': {'mk1': 'mv3'}})

    def test_instance_update_and_get_original_expected_host(self):
        # Ensure that we allow update when expecting a host field
        instance = self.create_instance_with_args()

        (orig, new) = db.instance_update_and_get_original(
            self.ctxt, instance['uuid'], {'host': None},
            expected={'host': 'h1'})

        self.assertIsNone(new['host'])

    def test_instance_update_and_get_original_expected_host_fail(self):
        # Ensure that we detect a changed expected host and raise
        # InstanceUpdateConflict
        instance = self.create_instance_with_args()

        try:
            db.instance_update_and_get_original(
                self.ctxt, instance['uuid'], {'host': None},
                expected={'host': 'h2'})
        except exception.InstanceUpdateConflict as ex:
            self.assertEqual(ex.kwargs['instance_uuid'], instance['uuid'])
            self.assertEqual(ex.kwargs['actual'], {'host': 'h1'})
            self.assertEqual(ex.kwargs['expected'], {'host': ['h2']})
        else:
            self.fail('InstanceUpdateConflict was not raised')

    # def test_instance_update_and_get_original_expected_host_none(self):
    #     # Ensure that we allow update when expecting a host field of None
    #     instance = self.create_instance_with_args(host=None)
    #
    #     (old, new) = db.instance_update_and_get_original(
    #         self.ctxt, instance['uuid'], {'host': 'h1'},
    #         expected={'host': None})
    #     self.assertEqual('h1', new['host'])
    #
    # def test_instance_update_and_get_original_expected_host_none_fail(self):
    #     # Ensure that we detect a changed expected host of None and raise
    #     # InstanceUpdateConflict
    #     instance = self.create_instance_with_args()
    #
    #     try:
    #         db.instance_update_and_get_original(
    #             self.ctxt, instance['uuid'], {'host': None},
    #             expected={'host': None})
    #     except exception.InstanceUpdateConflict as ex:
    #         self.assertEqual(ex.kwargs['instance_uuid'], instance['uuid'])
    #         self.assertEqual(ex.kwargs['actual'], {'host': 'h1'})
    #         self.assertEqual(ex.kwargs['expected'], {'host': [None]})
    #     else:
    #         self.fail('InstanceUpdateConflict was not raised')
    #
    # def test_instance_update_and_get_original_expected_task_state_single_fail(self):  # noqa
    #     # Ensure that we detect a changed expected task and raise
    #     # UnexpectedTaskStateError
    #     instance = self.create_instance_with_args()
    #
    #     try:
    #         db.instance_update_and_get_original(
    #             self.ctxt, instance['uuid'], {
    #                 'host': None,
    #                 'expected_task_state': task_states.SCHEDULING
    #             })
    #     except exception.UnexpectedTaskStateError as ex:
    #         self.assertEqual(ex.kwargs['instance_uuid'], instance['uuid'])
    #         self.assertEqual(ex.kwargs['actual'], {'task_state': None})
    #         self.assertEqual(ex.kwargs['expected'],
    #                          {'task_state': [task_states.SCHEDULING]})
    #     else:
    #         self.fail('UnexpectedTaskStateError was not raised')
    #
    # def test_instance_update_and_get_original_expected_task_state_single_pass(self):  # noqa
    #     # Ensure that we allow an update when expected task is correct
    #     instance = self.create_instance_with_args()
    #
    #     (orig, new) = db.instance_update_and_get_original(
    #         self.ctxt, instance['uuid'], {
    #             'host': None,
    #             'expected_task_state': None
    #         })
    #     self.assertIsNone(new['host'])

    # def test_instance_update_and_get_original_expected_task_state_multi_fail(self):  # noqa
    #     # Ensure that we detect a changed expected task and raise
    #     # UnexpectedTaskStateError when there are multiple potential expected
    #     # tasks
    #     instance = self.create_instance_with_args()
    #
    #     try:
    #         db.instance_update_and_get_original(
    #             self.ctxt, instance['uuid'], {
    #                 'host': None,
    #                 'expected_task_state': [task_states.SCHEDULING,
    #                                         task_states.REBUILDING]
    #             })
    #     except exception.UnexpectedTaskStateError as ex:
    #         self.assertEqual(ex.kwargs['instance_uuid'], instance['uuid'])
    #         self.assertEqual(ex.kwargs['actual'], {'task_state': None})
    #         self.assertEqual(ex.kwargs['expected'],
    #                          {'task_state': [task_states.SCHEDULING,
    #                                           task_states.REBUILDING]})
    #     else:
    #         self.fail('UnexpectedTaskStateError was not raised')

    # def test_instance_update_and_get_original_expected_task_state_multi_pass(self):  # noqa
    #     # Ensure that we allow an update when expected task is in a list of
    #     # expected tasks
    #     instance = self.create_instance_with_args()
    #
    #     (orig, new) = db.instance_update_and_get_original(
    #         self.ctxt, instance['uuid'], {
    #             'host': None,
    #             'expected_task_state': [task_states.SCHEDULING, None]
    #         })
    #     self.assertIsNone(new['host'])

    # def test_instance_update_and_get_original_expected_task_state_deleting(self):  # noqa
    #     # Ensure that we raise UnepectedDeletingTaskStateError when task state
    #     # is not as expected, and it is DELETING
    #     instance = self.create_instance_with_args(
    #         task_state=task_states.DELETING)
    #
    #     try:
    #         db.instance_update_and_get_original(
    #             self.ctxt, instance['uuid'], {
    #                 'host': None,
    #                 'expected_task_state': task_states.SCHEDULING
    #             })
    #     except exception.UnexpectedDeletingTaskStateError as ex:
    #         self.assertEqual(ex.kwargs['instance_uuid'], instance['uuid'])
    #         self.assertEqual(ex.kwargs['actual'],
    #                          {'task_state': task_states.DELETING})
    #         self.assertEqual(ex.kwargs['expected'],
    #                          {'task_state': [task_states.SCHEDULING]})
    #     else:
    #         self.fail('UnexpectedDeletingTaskStateError was not raised')

    # def test_instance_update_unique_name(self):
    #     context1 = context.RomeRequestContext('user1', 'p1')
    #     context2 = context.RomeRequestContext('user2', 'p2')
    #
    #     inst1 = self.create_instance_with_args(context=context1,
    #                                            project_id='p1',
    #                                            hostname='fake_name1')
    #     inst2 = self.create_instance_with_args(context=context1,
    #                                            project_id='p1',
    #                                            hostname='fake_name2')
    #     inst3 = self.create_instance_with_args(context=context2,
    #                                            project_id='p2',
    #                                            hostname='fake_name3')
    #     # osapi_compute_unique_server_name_scope is unset so this should work:
    #     db.instance_update(context1, inst1['uuid'], {'hostname': 'fake_name2'})
    #     db.instance_update(context1, inst1['uuid'], {'hostname': 'fake_name1'})
    #
    #     # With scope 'global' any duplicate should fail.
    #     self.flags(osapi_compute_unique_server_name_scope='global')
    #     self.assertRaises(exception.InstanceExists,
    #                       db.instance_update,
    #                       context1,
    #                       inst2['uuid'],
    #                       {'hostname': 'fake_name1'})
    #     self.assertRaises(exception.InstanceExists,
    #                       db.instance_update,
    #                       context2,
    #                       inst3['uuid'],
    #                       {'hostname': 'fake_name1'})
    #     # But we should definitely be able to update our name if we aren't
    #     #  really changing it.
    #     db.instance_update(context1, inst1['uuid'], {'hostname': 'fake_NAME'})
    #
    #     # With scope 'project' a duplicate in the project should fail:
    #     self.flags(osapi_compute_unique_server_name_scope='project')
    #     self.assertRaises(exception.InstanceExists, db.instance_update,
    #                       context1, inst2['uuid'], {'hostname': 'fake_NAME'})
    #
    #     # With scope 'project' a duplicate in a different project should work:
    #     self.flags(osapi_compute_unique_server_name_scope='project')
    #     db.instance_update(context2, inst3['uuid'], {'hostname': 'fake_NAME'})

    def _test_instance_update_updates_metadata(self, metadata_type):
        instance = self.create_instance_with_args()

        def set_and_check(meta):
            inst = db.instance_update(self.ctxt, instance['uuid'],
                               {metadata_type: dict(meta)})
            _meta = utils.metadata_to_dict(inst[metadata_type])
            self.assertEqual(meta, _meta)

        meta = {'speed': '88', 'units': 'MPH'}
        set_and_check(meta)
        meta['gigawatts'] = '1.21'
        set_and_check(meta)
        del meta['gigawatts']
        set_and_check(meta)
        self.ctxt.read_deleted = 'yes'
        self.assertNotIn('gigawatts',
            db.instance_system_metadata_get(self.ctxt, instance.uuid))

    def test_security_group_in_use(self):
        db.instance_create(self.ctxt, dict(host='foo'))

    def test_instance_update_updates_system_metadata(self):
        # Ensure that system_metadata is updated during instance_update
        self._test_instance_update_updates_metadata('system_metadata')

    def test_instance_update_updates_metadata(self):
        # Ensure that metadata is updated during instance_update
        self._test_instance_update_updates_metadata('metadata')

    def test_instance_floating_address_get_all(self):
        ctxt = context.get_admin_context()

        instance1 = db.instance_create(ctxt, {'host': 'h1', 'hostname': 'n1'})
        instance2 = db.instance_create(ctxt, {'host': 'h2', 'hostname': 'n2'})

        fixed_addresses = ['1.1.1.1', '1.1.1.2', '1.1.1.3']
        float_addresses = ['2.1.1.1', '2.1.1.2', '2.1.1.3']
        instance_uuids = [instance1['uuid'], instance1['uuid'],
                          instance2['uuid']]

        for fixed_addr, float_addr, instance_uuid in zip(fixed_addresses,
                                                         float_addresses,
                                                         instance_uuids):
            db.fixed_ip_create(ctxt, {'address': fixed_addr,
                                      'instance_uuid': instance_uuid})
            fixed_id = db.fixed_ip_get_by_address(ctxt, fixed_addr)['id']
            db.floating_ip_create(ctxt,
                                  {'address': float_addr,
                                   'fixed_ip_id': fixed_id})

        real_float_addresses = \
                db.instance_floating_address_get_all(ctxt, instance_uuids[0])
        self.assertEqual(set(float_addresses[:2]), set(real_float_addresses))
        real_float_addresses = \
                db.instance_floating_address_get_all(ctxt, instance_uuids[2])
        self.assertEqual(set([float_addresses[2]]), set(real_float_addresses))

        self.assertRaises(exception.InvalidUUID,
                          db.instance_floating_address_get_all,
                          ctxt, 'invalid_uuid')

    def test_instance_stringified_ips(self):
        instance = self.create_instance_with_args()
        instance = db.instance_update(
            self.ctxt, instance['uuid'],
            {'access_ip_v4': netaddr.IPAddress('1.2.3.4'),
             'access_ip_v6': netaddr.IPAddress('::1')})
        self.assertIsInstance(instance['access_ip_v4'], six.string_types)
        self.assertIsInstance(instance['access_ip_v6'], six.string_types)
        instance = db.instance_get_by_uuid(self.ctxt, instance['uuid'])
        self.assertIsInstance(instance['access_ip_v4'], six.string_types)
        self.assertIsInstance(instance['access_ip_v6'], six.string_types)

    # @mock.patch('nova.db.sqlalchemy.api._check_instance_exists_in_project',
    #             return_value=None)
    # def test_instance_destroy(self, mock_check_inst_exists):
    #     ctxt = context.get_admin_context()
    #     values = {
    #         'metadata': {'key': 'value'},
    #         'system_metadata': {'key': 'value'}
    #     }
    #     inst_uuid = self.create_instance_with_args(**values)['uuid']
    #     db.instance_tag_set(ctxt, inst_uuid, [u'tag1', u'tag2'])
    #     db.instance_destroy(ctxt, inst_uuid)
    #
    #     self.assertRaises(exception.InstanceNotFound,
    #                       db.instance_get, ctxt, inst_uuid)
    #     self.assertIsNone(db.instance_info_cache_get(ctxt, inst_uuid))
    #     self.assertEqual({}, db.instance_metadata_get(ctxt, inst_uuid))
    #     self.assertEqual([], db.instance_tag_get_by_instance_uuid(
    #         ctxt, inst_uuid))
    #     ctxt.read_deleted = 'yes'
    #     self.assertEqual(values['system_metadata'],
    #                      db.instance_system_metadata_get(ctxt, inst_uuid))

    def test_instance_destroy_already_destroyed(self):
        ctxt = context.get_admin_context()
        instance = self.create_instance_with_args()
        db.instance_destroy(ctxt, instance['uuid'])
        self.assertRaises(exception.InstanceNotFound,
                          db.instance_destroy, ctxt, instance['uuid'])

    def test_check_instance_exists(self):
        instance = self.create_instance_with_args()
        #with discovery_api.main_context_manager.reader.using(self.ctxt):
        self.assertIsNone(discovery_api._check_instance_exists_in_project(
            self.ctxt, instance['uuid']))

    def test_check_instance_exists_non_existing_instance(self):
        #with discovery_api.main_context_manager.reader.using(self.ctxt):
        self.assertRaises(exception.InstanceNotFound,
                          discovery_api._check_instance_exists_in_project,
                          self.ctxt, '123')

    def test_check_instance_exists_from_different_tenant(self):
        context1 = context.RomeRequestContext('user1', 'project1')
        context2 = context.RomeRequestContext('user2', 'project2')
        instance = self.create_instance_with_args(context=context1)
        #with discovery_api.main_context_manager.reader.using(context1):
        self.assertIsNone(discovery_api._check_instance_exists_in_project(
        context1, instance['uuid']))

        #with discovery_api.main_context_manager.reader.using(context2):
        self.assertRaises(exception.InstanceNotFound,
                          discovery_api._check_instance_exists_in_project,
                          context2, instance['uuid'])

    def test_check_instance_exists_admin_context(self):
        some_context = context.RomeRequestContext('some_user', 'some_project')
        instance = self.create_instance_with_args(context=some_context)

        #with discovery_api.main_context_manager.reader.using(self.ctxt):
        # Check that method works correctly with admin context
        self.assertIsNone(discovery_api._check_instance_exists_in_project(
            self.ctxt, instance['uuid']))


class InstanceMetadataTestCase(DiscoveryTestCase):

    """Tests for db.api.instance_metadata_* methods."""

    def setUp(self):
        super(InstanceMetadataTestCase, self).setUp()
        self.ctxt = context.get_admin_context()

    def test_instance_metadata_get(self):
        instance = db.instance_create(self.ctxt, {'metadata':
                                                    {'key': 'value'}})
        self.assertEqual({'key': 'value'}, db.instance_metadata_get(
                                            self.ctxt, instance['uuid']))

    def test_instance_metadata_delete(self):
        instance = db.instance_create(self.ctxt,
                                      {'metadata': {'key': 'val',
                                                    'key1': 'val1'}})
        db.instance_metadata_delete(self.ctxt, instance['uuid'], 'key1')
        self.assertEqual({'key': 'val'}, db.instance_metadata_get(
                                            self.ctxt, instance['uuid']))

    def test_instance_metadata_update(self):
        instance = db.instance_create(self.ctxt, {'host': 'h1',
                    'project_id': 'p1', 'metadata': {'key': 'value'}})

        # This should add new key/value pair
        db.instance_metadata_update(self.ctxt, instance['uuid'],
                                    {'new_key': 'new_value'}, False)
        metadata = db.instance_metadata_get(self.ctxt, instance['uuid'])
        self.assertEqual(metadata, {'key': 'value', 'new_key': 'new_value'})

        # This should leave only one key/value pair
        db.instance_metadata_update(self.ctxt, instance['uuid'],
                                    {'new_key': 'new_value'}, True)
        metadata = db.instance_metadata_get(self.ctxt, instance['uuid'])
        self.assertEqual(metadata, {'new_key': 'new_value'})

class ServiceTestCase(DiscoveryTestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(ServiceTestCase, self).setUp()
        self.ctxt = context.get_admin_context()

    def _get_base_values(self):
        return {
            'host': 'fake_host',
            'binary': 'fake_binary',
            'topic': 'fake_topic',
            'report_count': 3,
            'disabled': False,
            'forced_down': False
        }

    def _create_service(self, values):
        v = self._get_base_values()
        v.update(values)
        return db.service_create(self.ctxt, v)

    def test_service_create(self):
        service = self._create_service({})
        self.assertIsNotNone(service['id'])
        for key, value in self._get_base_values().items():
            self.assertEqual(value, service[key])

    def test_service_create_disabled(self):
        self.flags(enable_new_services=False)
        service = self._create_service({})
        self.assertTrue(service['disabled'])

    def test_service_destroy(self):
        service1 = self._create_service({})
        service2 = self._create_service({'host': 'fake_host2'})

        db.service_destroy(self.ctxt, service1['id'])
        self.assertRaises(exception.ServiceNotFound,
                          db.service_get, self.ctxt, service1['id'])
        self._assertEqualObjects(db.service_get(self.ctxt, service2['id']),
                                 service2, ignored_keys=['compute_node'])

    def test_service_update(self):
        service = self._create_service({})
        new_values = {
            'host': 'fake_host1',
            'binary': 'fake_binary1',
            'topic': 'fake_topic1',
            'report_count': 4,
            'disabled': True
        }
        db.service_update(self.ctxt, service['id'], new_values)
        updated_service = db.service_get(self.ctxt, service['id'])
        for key, value in new_values.items():
            self.assertEqual(value, updated_service[key])


    def test_service_update_not_found_exception(self):
        self.assertRaises(exception.ServiceNotFound,
                          db.service_update, self.ctxt, 100500, {})

    def test_service_update_with_set_forced_down(self):
        service = self._create_service({})
        db.service_update(self.ctxt, service['id'], {'forced_down': True})
        updated_service = db.service_get(self.ctxt, service['id'])
        self.assertTrue(updated_service['forced_down'])

    def test_service_update_with_unset_forced_down(self):
        service = self._create_service({'forced_down': True})
        db.service_update(self.ctxt, service['id'], {'forced_down': False})
        updated_service = db.service_get(self.ctxt, service['id'])
        self.assertFalse(updated_service['forced_down'])

    def test_service_get(self):
        service1 = self._create_service({})
        self._create_service({'host': 'some_other_fake_host'})
        real_service1 = db.service_get(self.ctxt, service1['id'])
        self._assertEqualObjects(service1, real_service1,
                                 ignored_keys=['compute_node'])

    def test_service_get_minimum_version(self):
        self._create_service({'version': 1,
                              'host': 'host3',
                              'binary': 'compute',
                              'forced_down': True})
        self._create_service({'version': 2,
                              'host': 'host1',
                              'binary': 'compute'})
        self._create_service({'version': 3,
                              'host': 'host2',
                              'binary': 'compute'})
        self.assertEqual(2, db.service_get_minimum_version(self.ctxt,
                                                           'compute'))

    def test_service_get_not_found_exception(self):
        self.assertRaises(exception.ServiceNotFound,
                          db.service_get, self.ctxt, 100500)

    def test_service_get_by_host_and_topic(self):
        service1 = self._create_service({'host': 'host1', 'topic': 'topic1'})
        self._create_service({'host': 'host2', 'topic': 'topic2'})

        real_service1 = db.service_get_by_host_and_topic(self.ctxt,
                                                         host='host1',
                                                         topic='topic1')
        self._assertEqualObjects(service1, real_service1)

    def test_service_get_by_host_and_binary(self):
        service1 = self._create_service({'host': 'host1', 'binary': 'foo'})
        self._create_service({'host': 'host2', 'binary': 'bar'})

        real_service1 = db.service_get_by_host_and_binary(self.ctxt,
                                                         host='host1',
                                                         binary='foo')
        self._assertEqualObjects(service1, real_service1)

    def test_service_get_by_host_and_binary_raises(self):
        self.assertRaises(exception.HostBinaryNotFound,
                          db.service_get_by_host_and_binary, self.ctxt,
                          host='host1', binary='baz')

    def test_service_get_all(self):
        values = [
            {'host': 'host1', 'topic': 'topic1'},
            {'host': 'host2', 'topic': 'topic2'},
            {'disabled': True}
        ]
        services = [self._create_service(vals) for vals in values]
        disabled_services = [services[-1]]
        non_disabled_services = services[:-1]

        compares = [
            (services, db.service_get_all(self.ctxt)),
            (disabled_services, db.service_get_all(self.ctxt, True)),
            (non_disabled_services, db.service_get_all(self.ctxt, False))
        ]
        for comp in compares:
            self._assertEqualListsOfObjects(*comp)

    def test_service_get_all_by_topic(self):
        values = [
            {'host': 'host1', 'topic': 't1'},
            {'host': 'host2', 'topic': 't1'},
            {'disabled': True, 'topic': 't1'},
            {'host': 'host3', 'topic': 't2'}
        ]
        services = [self._create_service(vals) for vals in values]
        expected = services[:2]
        real = db.service_get_all_by_topic(self.ctxt, 't1')
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_all_by_binary(self):
        values = [
            {'host': 'host1', 'binary': 'b1'},
            {'host': 'host2', 'binary': 'b1'},
            {'disabled': True, 'binary': 'b1'},
            {'host': 'host3', 'binary': 'b2'}
        ]
        services = [self._create_service(vals) for vals in values]
        expected = services[:2]
        real = db.service_get_all_by_binary(self.ctxt, 'b1')
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_all_by_binary_include_disabled(self):
        values = [
            {'host': 'host1', 'binary': 'b1'},
            {'host': 'host2', 'binary': 'b1'},
            {'disabled': True, 'binary': 'b1'},
            {'host': 'host3', 'binary': 'b2'}
        ]
        services = [self._create_service(vals) for vals in values]
        expected = services[:3]
        real = db.service_get_all_by_binary(self.ctxt, 'b1',
                                            include_disabled=True)
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_all_by_host(self):
        values = [
            {'host': 'host1', 'topic': 't11', 'binary': 'b11'},
            {'host': 'host1', 'topic': 't12', 'binary': 'b12'},
            {'host': 'host2', 'topic': 't1'},
            {'host': 'host3', 'topic': 't1'}
        ]
        services = [self._create_service(vals) for vals in values]

        expected = services[:2]
        real = db.service_get_all_by_host(self.ctxt, 'host1')
        self._assertEqualListsOfObjects(expected, real)

    def test_service_get_by_compute_host(self):
        values = [
            {'host': 'host1', 'binary': 'nova-compute'},
            {'host': 'host2', 'binary': 'nova-scheduler'},
            {'host': 'host3', 'binary': 'nova-compute'}
        ]
        services = [self._create_service(vals) for vals in values]

        real_service = db.service_get_by_compute_host(self.ctxt, 'host1')
        self._assertEqualObjects(services[0], real_service)

        self.assertRaises(exception.ComputeHostNotFound,
                          db.service_get_by_compute_host,
                          self.ctxt, 'non-exists-host')

    def test_service_get_by_compute_host_not_found(self):
        self.assertRaises(exception.ComputeHostNotFound,
                          db.service_get_by_compute_host,
                          self.ctxt, 'non-exists-host')

    def test_service_binary_exists_exception(self):
        db.service_create(self.ctxt, self._get_base_values())
        values = self._get_base_values()
        values.update({'topic': 'top1'})
        self.assertRaises(exception.ServiceBinaryExists, db.service_create,
                          self.ctxt, values)

    def test_service_topic_exists_exceptions(self):
        db.service_create(self.ctxt, self._get_base_values())
        values = self._get_base_values()
        values.update({'binary': 'bin1'})
        self.assertRaises(exception.ServiceTopicExists, db.service_create,
                          self.ctxt, values)


class BaseInstanceTypeTestCase(DiscoveryTestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(BaseInstanceTypeTestCase, self).setUp()
        self.ctxt = context.get_admin_context()
        self.user_ctxt = context.RomeRequestContext('user', 'user')


    def _get_base_values(self):
        return {
            'name': 'fake_name',
            'memory_mb': 512,
            'vcpus': 1,
            'root_gb': 10,
            'ephemeral_gb': 10,
            'flavorid': 'fake_flavor',
            'swap': 0,
            'rxtx_factor': 0.5,
            'vcpu_weight': 1,
            'disabled': False,
            'is_public': True
        }

    def _create_flavor(self, values, projects=None):
        v = self._get_base_values()
        v.update(values)
        return db.flavor_create(self.ctxt, v, projects)

class SecurityGroupRuleTestCase(DiscoveryTestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(SecurityGroupRuleTestCase, self).setUp()
        self.ctxt = context.get_admin_context()

    def _get_base_values(self):
        return {
            'name': 'fake_sec_group',
            'description': 'fake_sec_group_descr',
            'user_id': 'fake',
            'project_id': 'fake',
            'instances': []
            }

    def _get_base_rule_values(self):
        return {
            'protocol': "tcp",
            'from_port': 80,
            'to_port': 8080,
            'cidr': None,
            'deleted': 0,
            'deleted_at': None,
            'grantee_group': None,
            'updated_at': None
            }

    def _create_security_group(self, values):
        v = self._get_base_values()
        v.update(values)
        return db.security_group_create(self.ctxt, v)

    def _create_security_group_rule(self, values):
        v = self._get_base_rule_values()
        v.update(values)
        return db.security_group_rule_create(self.ctxt, v)

    def test_security_group_rule_create(self):
        security_group_rule = self._create_security_group_rule({})
        self.assertIsNotNone(security_group_rule['id'])
        for key, value in self._get_base_rule_values().items():
            self.assertEqual(value, security_group_rule[key])

    def _test_security_group_rule_get_by_security_group(self, columns=None):
        instance = db.instance_create(self.ctxt,
                                      {'system_metadata': {'foo': 'bar'}})
        security_group = self._create_security_group({
                'instances': [instance]})
        security_group_rule = self._create_security_group_rule(
            {'parent_group': security_group, 'grantee_group': security_group})
        security_group_rule1 = self._create_security_group_rule(
            {'parent_group': security_group, 'grantee_group': security_group})
        found_rules = db.security_group_rule_get_by_security_group(
            self.ctxt, security_group['id'], columns_to_join=columns)
        self.assertEqual(len(found_rules), 2)
        rules_ids = [security_group_rule['id'], security_group_rule1['id']]
        for rule in found_rules:
            if columns is None:
                self.assertIn('grantee_group', dict(rule))
                self.assertIn('instances',
                              dict(rule.grantee_group))
                self.assertIn(
                    'system_metadata',
                    dict(rule.grantee_group.instances[0]))
                self.assertIn(rule['id'], rules_ids)
            else:
                self.assertNotIn('grantee_group', dict(rule))

    def test_security_group_rule_get_by_security_group(self):
        self._test_security_group_rule_get_by_security_group()

    def test_security_group_rule_get_by_security_group_no_joins(self):
        self._test_security_group_rule_get_by_security_group(columns=[])

    def test_security_group_rule_get_by_instance(self):
        instance = db.instance_create(self.ctxt, {})
        security_group = self._create_security_group({
                'instances': [instance]})
        security_group_rule = self._create_security_group_rule(
            {'parent_group': security_group, 'grantee_group': security_group})
        security_group_rule1 = self._create_security_group_rule(
            {'parent_group': security_group, 'grantee_group': security_group})


        security_group_rule_ids = [security_group_rule['id'],
                                   security_group_rule1['id']]
        found_rules = db.security_group_rule_get_by_instance(self.ctxt,
                                                             instance['uuid'])
        self.assertEqual(len(found_rules), 2)
        for rule in found_rules:
            self.assertIn('grantee_group', rule)
            self.assertIn(rule['id'], security_group_rule_ids)

    def test_security_group_rule_destroy(self):
        self._create_security_group({'name': 'fake1'})
        self._create_security_group({'name': 'fake2'})
        security_group_rule1 = self._create_security_group_rule({})
        security_group_rule2 = self._create_security_group_rule({})
        db.security_group_rule_destroy(self.ctxt, security_group_rule1['id'])
        self.assertRaises(exception.SecurityGroupNotFound,
                          db.security_group_rule_get,
                          self.ctxt, security_group_rule1['id'])
        self._assertEqualObjects(db.security_group_rule_get(self.ctxt,
                                        security_group_rule2['id']),
                                 security_group_rule2, ['grantee_group'])

    def test_security_group_rule_destroy_not_found_exception(self):
        self.assertRaises(exception.SecurityGroupNotFound,
                          db.security_group_rule_destroy, self.ctxt, 100500)

    def test_security_group_rule_get(self):
        security_group_rule1 = (
                self._create_security_group_rule({}))
        self._create_security_group_rule({})
        real_security_group_rule = db.security_group_rule_get(self.ctxt,
                                              security_group_rule1['id'])
        self._assertEqualObjects(security_group_rule1,
                                 real_security_group_rule, ['grantee_group'])

    def test_security_group_rule_get_not_found_exception(self):
        self.assertRaises(exception.SecurityGroupNotFound,
                          db.security_group_rule_get, self.ctxt, 100500)

    def test_security_group_rule_count_by_group(self):
        sg1 = self._create_security_group({'name': 'fake1'})
        sg2 = self._create_security_group({'name': 'fake2'})
        rules_by_group = {sg1: [], sg2: []}
        for group in rules_by_group:
            rules = rules_by_group[group]
            for i in range(0, 10):
                rules.append(
                    self._create_security_group_rule({'parent_group_id':
                                                    group['id']}))
        db.security_group_rule_destroy(self.ctxt,
                                       rules_by_group[sg1][0]['id'])
        counted_groups = [db.security_group_rule_count_by_group(self.ctxt,
                                                                group['id'])
                          for group in [sg1, sg2]]
        expected = [9, 10]
        self.assertEqual(counted_groups, expected)

class SecurityGroupTestCase(DiscoveryTestCase, ModelsObjectComparatorMixin):
    def setUp(self):
        super(SecurityGroupTestCase, self).setUp()
        self.ctxt = context.get_admin_context()

    def _get_base_values(self):
        return {
            'name': 'fake_sec_group',
            'description': 'fake_sec_group_descr',
            'user_id': 'fake',
            'project_id': 'fake',
            'instances': []
            }

    def _create_security_group(self, values):
        v = self._get_base_values()
        v.update(values)
        return db.security_group_create(self.ctxt, v)

    def test_security_group_create(self):
        security_group = self._create_security_group({})
        self.assertIsNotNone(security_group['id'])
        for key, value in self._get_base_values().items():
            self.assertEqual(value, security_group[key])

    def test_security_group_destroy(self):
        security_group1 = self._create_security_group({})
        security_group2 = \
            self._create_security_group({'name': 'fake_sec_group2'})

        db.security_group_destroy(self.ctxt, security_group1['id'])
        self.assertRaises(exception.SecurityGroupNotFound,
                          db.security_group_get,
                          self.ctxt, security_group1['id'])
        self._assertEqualObjects(db.security_group_get(
                self.ctxt, security_group2['id'],
                columns_to_join=['instances']), security_group2)

    def test_security_group_get(self):
        security_group1 = self._create_security_group({})
        self._create_security_group({'name': 'fake_sec_group2'})
        real_security_group = db.security_group_get(self.ctxt,
                                              security_group1['id'],
                                              columns_to_join=['instances'])
        self._assertEqualObjects(security_group1,
                                 real_security_group)

    def test_security_group_get_with_instance_columns(self):
        instance = db.instance_create(self.ctxt,
                                      {'system_metadata': {'foo': 'bar'}})
        secgroup = self._create_security_group({'instances': [instance]})
        secgroup = db.security_group_get(
            self.ctxt, secgroup['id'],
            columns_to_join=['instances.system_metadata'])
        inst = secgroup.instances[0]
        self.assertIn('system_metadata', dict(inst).keys())

    def test_security_group_get_no_instances(self):
        instance = db.instance_create(self.ctxt, {})
        sid = self._create_security_group({'instances': [instance]})['id']

        security_group = db.security_group_get(self.ctxt, sid,
                                               columns_to_join=['instances'])
        self.assertIn('instances', security_group.__dict__)

        security_group = db.security_group_get(self.ctxt, sid)
        self.assertNotIn('instances', security_group.__dict__)

    def test_security_group_get_not_found_exception(self):
        self.assertRaises(exception.SecurityGroupNotFound,
                          db.security_group_get, self.ctxt, 100500)

    def test_security_group_get_by_name(self):
        security_group1 = self._create_security_group({'name': 'fake1'})
        security_group2 = self._create_security_group({'name': 'fake2'})

        real_security_group1 = db.security_group_get_by_name(
                                self.ctxt,
                                security_group1['project_id'],
                                security_group1['name'],
                                columns_to_join=None)
        real_security_group2 = db.security_group_get_by_name(
                                self.ctxt,
                                security_group2['project_id'],
                                security_group2['name'],
                                columns_to_join=None)
        self._assertEqualObjects(security_group1, real_security_group1)
        self._assertEqualObjects(security_group2, real_security_group2)

    def test_security_group_get_by_project(self):
        security_group1 = self._create_security_group(
                {'name': 'fake1', 'project_id': 'fake_proj1'})
        security_group2 = self._create_security_group(
                {'name': 'fake2', 'project_id': 'fake_proj2'})

        real1 = db.security_group_get_by_project(
                               self.ctxt,
                               security_group1['project_id'])
        real2 = db.security_group_get_by_project(
                               self.ctxt,
                               security_group2['project_id'])

        expected1, expected2 = [security_group1], [security_group2]
        self._assertEqualListsOfObjects(expected1, real1,
                                        ignored_keys=['instances'])
        self._assertEqualListsOfObjects(expected2, real2,
                                        ignored_keys=['instances'])

    def test_security_group_get_by_instance(self):
        instance = db.instance_create(self.ctxt, dict(host='foo'))
        values = [
            {'name': 'fake1', 'instances': [instance]},
            {'name': 'fake2', 'instances': [instance]},
            {'name': 'fake3', 'instances': []},
        ]
        security_groups = [self._create_security_group(vals)
                           for vals in values]

        real = db.security_group_get_by_instance(self.ctxt,
                                                 instance['uuid'])
        expected = security_groups[:2]
        self._assertEqualListsOfObjects(expected, real,
                                        ignored_keys=['instances'])

    def test_security_group_get_all(self):
        values = [
            {'name': 'fake1', 'project_id': 'fake_proj1'},
            {'name': 'fake2', 'project_id': 'fake_proj2'},
        ]
        security_groups = [self._create_security_group(vals)
                           for vals in values]

        real = db.security_group_get_all(self.ctxt)

        self._assertEqualListsOfObjects(security_groups, real,
                                        ignored_keys=['instances'])

    def test_security_group_in_use(self):
        instance = db.instance_create(self.ctxt, dict(host='foo'))
        values = [
            {'instances': [instance],
             'name': 'fake_in_use'},
            {'instances': []},
        ]

        security_groups = [self._create_security_group(vals)
                           for vals in values]

        real = []
        for security_group in security_groups:
            in_use = db.security_group_in_use(self.ctxt,
                                              security_group['id'])
            real.append(in_use)
        expected = [True, False]

        self.assertEqual(expected, real)

    def test_security_group_ensure_default(self):
        self.ctxt.project_id = 'fake'
        self.ctxt.user_id = 'fake'
        self.assertEqual(0, len(db.security_group_get_by_project(
                                    self.ctxt,
                                    self.ctxt.project_id)))

        db.security_group_ensure_default(self.ctxt)

        security_groups = db.security_group_get_by_project(
                            self.ctxt,
                            self.ctxt.project_id)

        self.assertEqual(1, len(security_groups))
        self.assertEqual("default", security_groups[0]["name"])

        usage = db.quota_usage_get(self.ctxt,
                                   self.ctxt.project_id,
                                   'security_groups',
                                   self.ctxt.user_id)
        self.assertEqual(1, usage.in_use)

    def test_security_group_ensure_default_until_refresh(self):
        self.flags(until_refresh=2)
        self.ctxt.project_id = 'fake'
        self.ctxt.user_id = 'fake'
        db.security_group_ensure_default(self.ctxt)
        usage = db.quota_usage_get(self.ctxt,
                                   self.ctxt.project_id,
                                   'security_groups',
                                   self.ctxt.user_id)
        self.assertEqual(2, usage.until_refresh)

    @mock.patch.object(discovery_api, '_security_group_get_by_names')
    def test_security_group_ensure_default_called_concurrently(self, sg_mock):
        # make sure NotFound is always raised here to trick Nova to insert the
        # duplicate security group entry
        sg_mock.side_effect = exception.NotFound

        # create the first db entry
        self.ctxt.project_id = 1
        db.security_group_ensure_default(self.ctxt)
        security_groups = db.security_group_get_by_project(
                            self.ctxt,
                            self.ctxt.project_id)
        self.assertEqual(1, len(security_groups))

        # create the second one and ensure the exception is handled properly
        default_group = db.security_group_ensure_default(self.ctxt)
        self.assertEqual('default', default_group.name)

    def test_security_group_update(self):
        security_group = self._create_security_group({})
        new_values = {
                    'name': 'sec_group1',
                    'description': 'sec_group_descr1',
                    'user_id': 'fake_user1',
                    'project_id': 'fake_proj1',
        }

        updated_group = db.security_group_update(self.ctxt,
                                    security_group['id'],
                                    new_values,
                                    columns_to_join=['rules.grantee_group'])
        for key, value in new_values.items():
            self.assertEqual(updated_group[key], value)
        self.assertEqual(updated_group['rules'], [])

    def test_security_group_update_to_duplicate(self):
        self._create_security_group(
                {'name': 'fake1', 'project_id': 'fake_proj1'})
        security_group2 = self._create_security_group(
                {'name': 'fake1', 'project_id': 'fake_proj2'})

        self.assertRaises(exception.SecurityGroupExists,
                          db.security_group_update,
                          self.ctxt, security_group2['id'],
                          {'project_id': 'fake_proj1'})

class InstanceTypeTestCase(BaseInstanceTypeTestCase):

    def test_flavor_create(self):
        flavor = self._create_flavor({})
        ignored_keys = ['id', 'deleted', 'deleted_at', 'updated_at',
                        'created_at', 'extra_specs']

        self.assertIsNotNone(flavor['id'])
        self._assertEqualObjects(flavor, self._get_base_values(),
                                 ignored_keys)

    def test_flavor_create_with_projects(self):
        projects = ['fake-project1', 'fake-project2']
        flavor = self._create_flavor({}, projects + ['fake-project2'])
        access = db.flavor_access_get_by_flavor_id(self.ctxt,
                                                   flavor['flavorid'])
        self.assertEqual(projects, [x.project_id for x in access])

    def test_flavor_destroy(self):
        specs1 = {'a': '1', 'b': '2'}
        flavor1 = self._create_flavor({'name': 'name1', 'flavorid': 'a1',
                                       'extra_specs': specs1})
        specs2 = {'c': '4', 'd': '3'}
        flavor2 = self._create_flavor({'name': 'name2', 'flavorid': 'a2',
                                       'extra_specs': specs2})

        db.flavor_destroy(self.ctxt, 'name1')

        self.assertRaises(exception.FlavorNotFound,
                          db.flavor_get, self.ctxt, flavor1['id'])
        real_specs1 = db.flavor_extra_specs_get(self.ctxt, flavor1['flavorid'])
        self._assertEqualObjects(real_specs1, {})

        r_flavor2 = db.flavor_get(self.ctxt, flavor2['id'])
        self._assertEqualObjects(flavor2, r_flavor2, 'extra_specs')

    def test_flavor_destroy_not_found(self):
        self.assertRaises(exception.FlavorNotFound,
                          db.flavor_destroy, self.ctxt, 'nonexists')

    def test_flavor_create_duplicate_name(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorExists,
                          self._create_flavor,
                          {'flavorid': 'some_random_flavor'})

    def test_flavor_create_duplicate_flavorid(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorIdExists,
                          self._create_flavor,
                          {'name': 'some_random_name'})

    def test_flavor_create_with_extra_specs(self):
        extra_specs = dict(a='abc', b='def', c='ghi')
        flavor = self._create_flavor({'extra_specs': extra_specs})
        ignored_keys = ['id', 'deleted', 'deleted_at', 'updated_at',
                        'created_at', 'extra_specs']

        self._assertEqualObjects(flavor, self._get_base_values(),
                                 ignored_keys)
        self._assertEqualObjects(extra_specs, flavor['extra_specs'])

    @mock.patch('lib.rome.core.orm.query.Query.all', return_value=[])
    def test_flavor_create_with_extra_specs_duplicate(self, mock_all):
        extra_specs = dict(key='value')
        flavorid = 'flavorid'
        self._create_flavor({'flavorid': flavorid, 'extra_specs': extra_specs})

        self.assertRaises(exception.FlavorExtraSpecUpdateCreateFailed,
                          db.flavor_extra_specs_update_or_create,
                          self.ctxt, flavorid, extra_specs)

    def test_flavor_get_all(self):
        # NOTE(boris-42): Remove base instance types
        for it in db.flavor_get_all(self.ctxt):
            db.flavor_destroy(self.ctxt, it['name'])

        flavors = [
            {'root_gb': 600, 'memory_mb': 100, 'disabled': True,
             'is_public': True, 'name': 'a1', 'flavorid': 'f1'},
            {'root_gb': 500, 'memory_mb': 200, 'disabled': True,
             'is_public': True, 'name': 'a2', 'flavorid': 'f2'},
            {'root_gb': 400, 'memory_mb': 300, 'disabled': False,
             'is_public': True, 'name': 'a3', 'flavorid': 'f3'},
            {'root_gb': 300, 'memory_mb': 400, 'disabled': False,
             'is_public': False, 'name': 'a4', 'flavorid': 'f4'},
            {'root_gb': 200, 'memory_mb': 500, 'disabled': True,
             'is_public': False, 'name': 'a5', 'flavorid': 'f5'},
            {'root_gb': 100, 'memory_mb': 600, 'disabled': True,
             'is_public': False, 'name': 'a6', 'flavorid': 'f6'}
        ]
        flavors = [self._create_flavor(it) for it in flavors]

        lambda_filters = {
            'min_memory_mb': lambda it, v: it['memory_mb'] >= v,
            'min_root_gb': lambda it, v: it['root_gb'] >= v,
            'disabled': lambda it, v: it['disabled'] == v,
            'is_public': lambda it, v: (v is None or it['is_public'] == v)
        }

        mem_filts = [{'min_memory_mb': x} for x in [100, 350, 550, 650]]
        root_filts = [{'min_root_gb': x} for x in [100, 350, 550, 650]]
        disabled_filts = [{'disabled': x} for x in [True, False]]
        is_public_filts = [{'is_public': x} for x in [True, False, None]]

        def assert_multi_filter_flavor_get(filters=None):
            if filters is None:
                filters = {}

            expected_it = flavors
            for name, value in filters.items():
                filt = lambda it: lambda_filters[name](it, value)
                expected_it = list(filter(filt, expected_it))

            real_it = db.flavor_get_all(self.ctxt, filters=filters)
            self._assertEqualListsOfObjects(expected_it, real_it)

        # no filter
        assert_multi_filter_flavor_get()

        # test only with one filter
        for filt in mem_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in root_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in disabled_filts:
            assert_multi_filter_flavor_get(filt)
        for filt in is_public_filts:
            assert_multi_filter_flavor_get(filt)

        # test all filters together
        for mem in mem_filts:
            for root in root_filts:
                for disabled in disabled_filts:
                    for is_public in is_public_filts:
                        filts = {}
                        for f in (mem, root, disabled, is_public):
                            filts.update(f)
                        assert_multi_filter_flavor_get(filts)

    def test_flavor_get_all_limit_sort(self):
        def assert_sorted_by_key_dir(sort_key, asc=True):
            sort_dir = 'asc' if asc else 'desc'
            results = db.flavor_get_all(self.ctxt, sort_key='name',
                                        sort_dir=sort_dir)
            # Manually sort the results as we would expect them
            expected_results = sorted(results,
                                      key=lambda item: item['name'],
                                      reverse=(not asc))
            self.assertEqual(expected_results, results)

        def assert_sorted_by_key_both_dir(sort_key):
            assert_sorted_by_key_dir(sort_key, True)
            assert_sorted_by_key_dir(sort_key, False)

        for attr in ['memory_mb', 'root_gb', 'deleted_at', 'name', 'deleted',
                     'created_at', 'ephemeral_gb', 'updated_at', 'disabled',
                     'vcpus', 'swap', 'rxtx_factor', 'is_public', 'flavorid',
                     'vcpu_weight', 'id']:
            assert_sorted_by_key_both_dir(attr)

    def test_flavor_get_all_limit(self):
        limited_flavors = db.flavor_get_all(self.ctxt, limit=2)
        self.assertEqual(2, len(limited_flavors))

    def test_flavor_get_all_list_marker(self):
        all_flavors = db.flavor_get_all(self.ctxt)

        # Set the 3rd result as the marker
        marker_flavorid = all_flavors[2]['flavorid']
        marked_flavors = db.flavor_get_all(self.ctxt, marker=marker_flavorid)
        # We expect everything /after/ the 3rd result
        expected_results = all_flavors[3:]
        self.assertEqual(expected_results, marked_flavors)

    def test_flavor_get_all_marker_not_found(self):
        self.assertRaises(exception.MarkerNotFound,
                db.flavor_get_all, self.ctxt, marker='invalid')

    def test_flavor_get(self):
        flavors = [{'name': 'abc', 'flavorid': '123'},
                   {'name': 'def', 'flavorid': '456'},
                   {'name': 'ghi', 'flavorid': '789'}]
        flavors = [self._create_flavor(t) for t in flavors]

        for flavor in flavors:
            flavor_by_id = db.flavor_get(self.ctxt, flavor['id'])
            self._assertEqualObjects(flavor, flavor_by_id)

    def test_flavor_get_non_public(self):
        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
                                      'is_public': False})

        # Admin can see it
        flavor_by_id = db.flavor_get(self.ctxt, flavor['id'])
        self._assertEqualObjects(flavor, flavor_by_id)

        # Regular user can not
        self.assertRaises(exception.FlavorNotFound, db.flavor_get,
                self.user_ctxt, flavor['id'])

        # Regular user can see it after being granted access
        db.flavor_access_add(self.ctxt, flavor['flavorid'],
                self.user_ctxt.project_id)
        flavor_by_id = db.flavor_get(self.user_ctxt, flavor['id'])
        self._assertEqualObjects(flavor, flavor_by_id)

    def test_flavor_get_by_name(self):
        flavors = [{'name': 'abc', 'flavorid': '123'},
                   {'name': 'def', 'flavorid': '456'},
                   {'name': 'ghi', 'flavorid': '789'}]
        flavors = [self._create_flavor(t) for t in flavors]

        for flavor in flavors:
            flavor_by_name = db.flavor_get_by_name(self.ctxt, flavor['name'])
            self._assertEqualObjects(flavor, flavor_by_name)

    def test_flavor_get_by_name_not_found(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorNotFoundByName,
                          db.flavor_get_by_name, self.ctxt, 'nonexists')

    def test_flavor_get_by_name_non_public(self):
        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
                                      'is_public': False})

        # Admin can see it
        flavor_by_name = db.flavor_get_by_name(self.ctxt, flavor['name'])
        self._assertEqualObjects(flavor, flavor_by_name)

        # Regular user can not
        self.assertRaises(exception.FlavorNotFoundByName,
                db.flavor_get_by_name, self.user_ctxt,
                flavor['name'])

        # Regular user can see it after being granted access
        db.flavor_access_add(self.ctxt, flavor['flavorid'],
                self.user_ctxt.project_id)
        flavor_by_name = db.flavor_get_by_name(self.user_ctxt, flavor['name'])
        self._assertEqualObjects(flavor, flavor_by_name)

    def test_flavor_get_by_flavor_id(self):
        flavors = [{'name': 'abc', 'flavorid': '123'},
                   {'name': 'def', 'flavorid': '456'},
                   {'name': 'ghi', 'flavorid': '789'}]
        flavors = [self._create_flavor(t) for t in flavors]

        for flavor in flavors:
            params = (self.ctxt, flavor['flavorid'])
            flavor_by_flavorid = db.flavor_get_by_flavor_id(*params)
            self._assertEqualObjects(flavor, flavor_by_flavorid)

    def test_flavor_get_by_flavor_not_found(self):
        self._create_flavor({})
        self.assertRaises(exception.FlavorNotFound,
                          db.flavor_get_by_flavor_id,
                          self.ctxt, 'nonexists')

    def test_flavor_get_by_flavor_id_non_public(self):
        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123',
                                      'is_public': False})

        # Admin can see it
        flavor_by_fid = db.flavor_get_by_flavor_id(self.ctxt,
                                                   flavor['flavorid'])
        self._assertEqualObjects(flavor, flavor_by_fid)

        # Regular user can not
        self.assertRaises(exception.FlavorNotFound,
                db.flavor_get_by_flavor_id, self.user_ctxt,
                flavor['flavorid'])

        # Regular user can see it after being granted access
        db.flavor_access_add(self.ctxt, flavor['flavorid'],
                self.user_ctxt.project_id)
        flavor_by_fid = db.flavor_get_by_flavor_id(self.user_ctxt,
                                                   flavor['flavorid'])
        self._assertEqualObjects(flavor, flavor_by_fid)

    def test_flavor_get_by_flavor_id_deleted(self):
        flavor = self._create_flavor({'name': 'abc', 'flavorid': '123'})

        db.flavor_destroy(self.ctxt, 'abc')

        flavor_by_fid = db.flavor_get_by_flavor_id(self.ctxt,
                flavor['flavorid'], read_deleted='yes')
        self.assertEqual(flavor['id'], flavor_by_fid['id'])

    def test_flavor_get_by_flavor_id_deleted_and_recreat(self):
        # NOTE(wingwj): Aims to test difference between mysql and postgresql
        # for bug 1288636
        param_dict = {'name': 'abc', 'flavorid': '123'}

        self._create_flavor(param_dict)
        db.flavor_destroy(self.ctxt, 'abc')

        # Recreate the flavor with the same params
        flavor = self._create_flavor(param_dict)

        flavor_by_fid = db.flavor_get_by_flavor_id(self.ctxt,
                flavor['flavorid'], read_deleted='yes')
        self.assertEqual(flavor['id'], flavor_by_fid['id'])

class NetworkTestCase(DiscoveryTestCase, ModelsObjectComparatorMixin):

    """Tests for db.api.network_* methods."""

    def setUp(self):
        super(NetworkTestCase, self).setUp()
        self.ctxt = context.get_admin_context()

    def _get_associated_fixed_ip(self, host, cidr, ip):
        network = db.network_create_safe(self.ctxt,
            {'project_id': 'project1', 'cidr': cidr})
        self.assertFalse(db.network_in_use_on_host(self.ctxt, network.id,
            host))
        instance = db.instance_create(self.ctxt,
            {'project_id': 'project1', 'host': host})
        virtual_interface = db.virtual_interface_create(self.ctxt,
            {'instance_uuid': instance.uuid, 'network_id': network.id,
            'address': ip})
        db.fixed_ip_create(self.ctxt, {'address': ip,
            'network_id': network.id, 'allocated': True,
            'virtual_interface_id': virtual_interface.id})
        db.fixed_ip_associate(self.ctxt, ip, instance.uuid,
            network.id, virtual_interface_id=virtual_interface['id'])
        return network, instance

    def test_network_get_associated_default_route(self):
        network, instance = self._get_associated_fixed_ip('host.net',
            '192.0.2.0/30', '192.0.2.1')
        network2 = db.network_create_safe(self.ctxt,
            {'project_id': 'project1', 'cidr': '192.0.3.0/30'})
        ip = '192.0.3.1'
        virtual_interface = db.virtual_interface_create(self.ctxt,
            {'instance_uuid': instance.uuid, 'network_id': network2.id,
            'address': ip})
        db.fixed_ip_create(self.ctxt, {'address': ip,
            'network_id': network2.id, 'allocated': True,
            'virtual_interface_id': virtual_interface.id})
        db.fixed_ip_associate(self.ctxt, ip, instance.uuid,
            network2.id)
        data = db.network_get_associated_fixed_ips(self.ctxt, network.id)
        self.assertEqual(1, len(data))
        self.assertTrue(data[0]['default_route'])
        data = db.network_get_associated_fixed_ips(self.ctxt, network2.id)
        self.assertEqual(1, len(data))
        self.assertFalse(data[0]['default_route'])

    def test_network_get_associated_fixed_ips(self):
        network, instance = self._get_associated_fixed_ip('host.net',
            '192.0.2.0/30', '192.0.2.1')
        data = db.network_get_associated_fixed_ips(self.ctxt, network.id)
        self.assertEqual(1, len(data))
        self.assertEqual('192.0.2.1', data[0]['address'])
        self.assertEqual('192.0.2.1', data[0]['vif_address'])
        self.assertEqual(instance.uuid, data[0]['instance_uuid'])
        self.assertTrue(data[0][fields.PciDeviceStatus.ALLOCATED])

    def test_network_create_safe(self):
        values = {'host': 'localhost', 'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        self.assertEqual(36, len(network['uuid']))
        db_network = db.network_get(self.ctxt, network['id'])
        self._assertEqualObjects(network, db_network)

    def test_network_create_with_duplicate_vlan(self):
        values1 = {'host': 'localhost', 'project_id': 'project1', 'vlan': 1}
        values2 = {'host': 'something', 'project_id': 'project1', 'vlan': 1}
        db.network_create_safe(self.ctxt, values1)
        self.assertRaises(exception.DuplicateVlan,
                          db.network_create_safe, self.ctxt, values2)

    def test_network_delete_safe(self):
        values = {'host': 'localhost', 'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        db.network_get(self.ctxt, network['id'])
        values = {'network_id': network['id'], 'address': '192.168.1.5'}
        address1 = db.fixed_ip_create(self.ctxt, values)['address']
        values = {'network_id': network['id'],
                  'address': '192.168.1.6',
                  'allocated': True}
        address2 = db.fixed_ip_create(self.ctxt, values)['address']
        self.assertRaises(exception.NetworkInUse,
                          db.network_delete_safe, self.ctxt, network['id'])
        db.fixed_ip_update(self.ctxt, address2, {'allocated': False})
        network = db.network_delete_safe(self.ctxt, network['id'])
        self.assertRaises(exception.FixedIpNotFoundForAddress,
                          db.fixed_ip_get_by_address, self.ctxt, address1)
        ctxt = self.ctxt.elevated(read_deleted='yes')
        fixed_ip = db.fixed_ip_get_by_address(ctxt, address1)
        self.assertTrue(fixed_ip['deleted'])

    def test_network_in_use_on_host(self):
        values = {'host': 'foo', 'hostname': 'myname'}
        instance = db.instance_create(self.ctxt, values)
        values = {'address': '192.168.1.5', 'instance_uuid': instance['uuid']}
        vif = db.virtual_interface_create(self.ctxt, values)
        values = {'address': '192.168.1.6',
                  'network_id': 1,
                  'allocated': True,
                  'instance_uuid': instance['uuid'],
                  'virtual_interface_id': vif['id']}
        db.fixed_ip_create(self.ctxt, values)
        self.assertTrue(db.network_in_use_on_host(self.ctxt, 1, 'foo'))
        self.assertFalse(db.network_in_use_on_host(self.ctxt, 1, 'bar'))

    def test_network_update_nonexistent(self):
        self.assertRaises(exception.NetworkNotFound,
            db.network_update, self.ctxt, 123456, {})

    def test_network_update_with_duplicate_vlan(self):
        values1 = {'host': 'localhost', 'project_id': 'project1', 'vlan': 1}
        values2 = {'host': 'something', 'project_id': 'project1', 'vlan': 2}
        network_ref = db.network_create_safe(self.ctxt, values1)
        db.network_create_safe(self.ctxt, values2)
        self.assertRaises(exception.DuplicateVlan,
                          db.network_update, self.ctxt,
                          network_ref["id"], values2)

    def test_network_update(self):
        network = db.network_create_safe(self.ctxt, {'project_id': 'project1',
            'vlan': 1, 'host': 'test.com'})
        db.network_update(self.ctxt, network.id, {'vlan': 2})
        network_new = db.network_get(self.ctxt, network.id)
        self.assertEqual(2, network_new.vlan)

    def test_network_set_host_nonexistent_network(self):
        self.assertRaises(exception.NetworkNotFound, db.network_set_host,
                          self.ctxt, 123456, 'nonexistent')

    def test_network_set_host_already_set_correct(self):
        values = {'host': 'example.com', 'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        self.assertIsNone(db.network_set_host(self.ctxt, network.id,
                          'example.com'))

    def test_network_set_host_already_set_incorrect(self):
        values = {'host': 'example.com', 'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        self.assertIsNone(db.network_set_host(self.ctxt, network.id,
                                              'new.example.com'))

    def test_network_set_host_with_initially_no_host(self):
        values = {'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)
        db.network_set_host(self.ctxt, network.id, 'example.com')
        self.assertEqual('example.com',
            db.network_get(self.ctxt, network.id).host)

    def test_network_set_host_succeeds_retry_on_deadlock(self):
        values = {'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)

        def fake_update(params):
            if mock_update.call_count == 1:
                raise db_exc.DBDeadlock()
            else:
                return 1

        with mock.patch('lib.rome.core.orm.query.Query.update',
                        side_effect=fake_update) as mock_update:
            db.network_set_host(self.ctxt, network.id, 'example.com')
            self.assertEqual(2, mock_update.call_count)

    def test_network_set_host_succeeds_retry_on_no_rows_updated(self):
        values = {'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)

        def fake_update(params):
            if mock_update.call_count == 1:
                return 0
            else:
                return 1

        with mock.patch('lib.rome.core.orm.query.Query.update',
                        side_effect=fake_update) as mock_update:
            db.network_set_host(self.ctxt, network.id, 'example.com')
            self.assertEqual(2, mock_update.call_count)

    def test_network_set_host_failed_with_retry_on_no_rows_updated(self):
        values = {'project_id': 'project1'}
        network = db.network_create_safe(self.ctxt, values)

        with mock.patch('lib.rome.core.orm.query.Query.update',
                        return_value=0) as mock_update:
            self.assertRaises(exception.NetworkSetHostFailed,
                              db.network_set_host, self.ctxt, network.id,
                              'example.com')
            # 5 retries + initial attempt
            self.assertEqual(6, mock_update.call_count)

    def test_network_get_all_by_host(self):
        self.assertEqual([],
            db.network_get_all_by_host(self.ctxt, 'example.com'))
        host = 'h1.example.com'
        # network with host set
        net1 = db.network_create_safe(self.ctxt, {'host': host})
        self._assertEqualListsOfObjects([net1],
            db.network_get_all_by_host(self.ctxt, host))
        # network with fixed ip with host set
        net2 = db.network_create_safe(self.ctxt, {})
        db.fixed_ip_create(self.ctxt, {'host': host, 'network_id': net2.id})
        # db.network_get_all_by_host(self.ctxt, host)
        # TODO(msimonin): the following fails because fixed_ips are nested
        # self._assertEqualListsOfObjects([net1, net2],
        #     db.network_get_all_by_host(self.ctxt, host))
        ntxs = db.network_get_all_by_host(self.ctxt, host)
        self.assertEquals(2, len(ntxs))
        self.assertListEqual([net1["id"], net2["id"]], [ntxs[0]["id"], ntxs[1]["id"]])
        # network with instance with host set
        net3 = db.network_create_safe(self.ctxt, {})
        instance = db.instance_create(self.ctxt, {'host': host})
        db.fixed_ip_create(self.ctxt, {'network_id': net3.id,
            'instance_uuid': instance.uuid})
        #self._assertEqualListsOfObjects([net1, net2, net3],
        #    db.network_get_all_by_host(self.ctxt, host))
        ntxs = db.network_get_all_by_host(self.ctxt, host)
        self.assertEquals(3, len(ntxs))
        self.assertListEqual([net1["id"], net2["id"], net3["id"]], [ntxs[0]["id"], ntxs[1]["id"], ntxs[2]["id"]])

    def test_network_get_by_cidr(self):
        cidr = '192.0.2.0/30'
        cidr_v6 = '2001:db8:1::/64'
        network = db.network_create_safe(self.ctxt,
            {'project_id': 'project1', 'cidr': cidr, 'cidr_v6': cidr_v6})
        self._assertEqualObjects(network,
            db.network_get_by_cidr(self.ctxt, cidr))
        self._assertEqualObjects(network,
            db.network_get_by_cidr(self.ctxt, cidr_v6))

    def test_network_get_by_cidr_nonexistent(self):
        self.assertRaises(exception.NetworkNotFoundForCidr,
            db.network_get_by_cidr, self.ctxt, '192.0.2.0/30')

    def test_network_get_by_uuid(self):
        network = db.network_create_safe(self.ctxt,
            {'project_id': 'project_1'})
        self._assertEqualObjects(network,
            db.network_get_by_uuid(self.ctxt, network.uuid))

    def test_network_get_by_uuid_nonexistent(self):
        self.assertRaises(exception.NetworkNotFoundForUUID,
            db.network_get_by_uuid, self.ctxt, 'non-existent-uuid')

    def test_network_get_all_by_uuids_no_networks(self):
        self.assertRaises(exception.NoNetworksFound,
            db.network_get_all_by_uuids, self.ctxt, ['non-existent-uuid'])

    def test_network_get_all_by_uuids(self):
        net1 = db.network_create_safe(self.ctxt, {})
        net2 = db.network_create_safe(self.ctxt, {})
        self._assertEqualListsOfObjects([net1, net2],
            db.network_get_all_by_uuids(self.ctxt, [net1.uuid, net2.uuid]))

    def test_network_get_all_no_networks(self):
        self.assertRaises(exception.NoNetworksFound,
            db.network_get_all, self.ctxt)

    def test_network_get_all(self):
        network = db.network_create_safe(self.ctxt, {})
        network_db = db.network_get_all(self.ctxt)
        self.assertEqual(1, len(network_db))
        self._assertEqualObjects(network, network_db[0])

    def test_network_get_all_admin_user(self):
        network1 = db.network_create_safe(self.ctxt, {})
        network2 = db.network_create_safe(self.ctxt,
                                          {'project_id': 'project1'})
        self._assertEqualListsOfObjects([network1, network2],
                                        db.network_get_all(self.ctxt,
                                                           project_only=True))

    def test_network_get_all_normal_user(self):
        normal_ctxt = context.RequestContext('fake', 'fake')
        db.network_create_safe(self.ctxt, {})
        db.network_create_safe(self.ctxt, {'project_id': 'project1'})
        network1 = db.network_create_safe(self.ctxt,
                                          {'project_id': 'fake'})
        network_db = db.network_get_all(normal_ctxt, project_only=True)
        self.assertEqual(1, len(network_db))
        self._assertEqualObjects(network1, network_db[0])

    def test_network_get(self):
        network = db.network_create_safe(self.ctxt, {})
        self._assertEqualObjects(db.network_get(self.ctxt, network.id),
            network)
        db.network_delete_safe(self.ctxt, network.id)
        self.assertRaises(exception.NetworkNotFound,
            db.network_get, self.ctxt, network.id)

    def test_network_associate(self):
        network = db.network_create_safe(self.ctxt, {})
        self.assertIsNone(network.project_id)
        db.network_associate(self.ctxt, "project1", network.id)
        self.assertEqual("project1", db.network_get(self.ctxt,
            network.id).project_id)

    def test_network_diassociate(self):
        network = db.network_create_safe(self.ctxt,
            {'project_id': 'project1', 'host': 'test.net'})
        # disassociate project
        db.network_disassociate(self.ctxt, network.id, False, True)
        self.assertIsNone(db.network_get(self.ctxt, network.id).project_id)
        # disassociate host
        db.network_disassociate(self.ctxt, network.id, True, False)
        self.assertIsNone(db.network_get(self.ctxt, network.id).host)

    def test_network_count_reserved_ips(self):
        net = db.network_create_safe(self.ctxt, {})
        self.assertEqual(0, db.network_count_reserved_ips(self.ctxt, net.id))
        db.fixed_ip_create(self.ctxt, {'network_id': net.id,
            'reserved': True})
        self.assertEqual(1, db.network_count_reserved_ips(self.ctxt, net.id))

class ComputeNodeTestCase(DiscoveryTestCase, ModelsObjectComparatorMixin):

    _ignored_keys = ['id', 'deleted', 'deleted_at', 'created_at', 'updated_at']
    # TODO(jaypipes): Remove once the compute node inventory migration has
    # been completed and the scheduler uses the inventories and allocations
    # tables directly.
    _ignored_temp_resource_providers_keys = [
        'inv_memory_mb',
        'inv_memory_mb_reserved',
        'inv_ram_allocation_ratio',
        'inv_memory_mb_used',
        'inv_vcpus',
        'inv_cpu_allocation_ratio',
        'inv_vcpus_used',
        'inv_local_gb',
        'inv_local_gb_reserved',
        'inv_disk_allocation_ratio',
        'inv_local_gb_used',
    ]

    def setUp(self):
        super(ComputeNodeTestCase, self).setUp()
        self.ctxt = context.get_admin_context()
        self.service_dict = dict(host='host1', binary='nova-compute',
                            topic=CONF.compute_topic, report_count=1,
                            disabled=False)
        self.service = db.service_create(self.ctxt, self.service_dict)
        self.compute_node_dict = dict(vcpus=2, memory_mb=1024, local_gb=2048,
                                 uuid=uuidsentinel.fake_compute_node,
                                 vcpus_used=0, memory_mb_used=0,
                                 local_gb_used=0, free_ram_mb=1024,
                                 free_disk_gb=2048, hypervisor_type="xen",
                                 hypervisor_version=1, cpu_info="",
                                 running_vms=0, current_workload=0,
                                 service_id=self.service['id'],
                                 host=self.service['host'],
                                 disk_available_least=100,
                                 hypervisor_hostname='abracadabra104',
                                 host_ip='127.0.0.1',
                                 supported_instances='',
                                 pci_stats='',
                                 metrics='',
                                 extra_resources='',
                                 cpu_allocation_ratio=16.0,
                                 ram_allocation_ratio=1.5,
                                 disk_allocation_ratio=1.0,
                                 stats='', numa_topology='')
        # add some random stats
        self.stats = dict(num_instances=3, num_proj_12345=2,
                     num_proj_23456=2, num_vm_building=3)
        self.compute_node_dict['stats'] = jsonutils.dumps(self.stats)
#        self.flags(reserved_host_memory_mb=0)
#        self.flags(reserved_host_disk_mb=0)
        self.item = db.compute_node_create(self.ctxt, self.compute_node_dict)

    def test_compute_node_create(self):
        self._assertEqualObjects(self.compute_node_dict, self.item,
                                ignored_keys=self._ignored_keys + ['stats'])
        new_stats = jsonutils.loads(self.item['stats'])
        self.assertEqual(self.stats, new_stats)

    def test_compute_node_get_all(self):
        nodes = db.compute_node_get_all(self.ctxt)
        self.assertEqual(1, len(nodes))
        node = nodes[0]
        self._assertEqualObjects(self.compute_node_dict, node,
                    ignored_keys=self._ignored_keys +
                                 self._ignored_temp_resource_providers_keys +
                                 ['stats', 'service'])
        new_stats = jsonutils.loads(node['stats'])
        self.assertEqual(self.stats, new_stats)

    # NOTE(disco/msimonin): skip new ressource provider for now.
    # def test_compute_node_select_schema(self):
    # def test_compute_node_select_schema(self):
    #     # We here test that compute nodes that have inventory and allocation
    #     # entries under the new resource-providers schema return non-None
    #     # values for the inv_* fields in the returned list of dicts from
    #     # _compute_node_select().
    #     nodes = sqlalchemy_api._compute_node_select(self.ctxt)
    #     self.assertEqual(1, len(nodes))
    #     node = nodes[0]
    #     self.assertIsNone(node['inv_memory_mb'])
    #     self.assertIsNone(node['inv_memory_mb_used'])
    #
    #     RAM_MB = fields.ResourceClass.index(fields.ResourceClass.MEMORY_MB)
    #     VCPU = fields.ResourceClass.index(fields.ResourceClass.VCPU)
    #     DISK_GB = fields.ResourceClass.index(fields.ResourceClass.DISK_GB)
    #
    #     @sqlalchemy_api.main_context_manager.writer
    #     def create_resource_provider(context):
    #         rp = models.ResourceProvider()
    #         rp.uuid = node['uuid']
    #         rp.save(context.session)
    #         return rp.id
    #
    #     @sqlalchemy_api.main_context_manager.writer
    #     def create_inventory(context, provider_id, resource_class, total):
    #         inv = models.Inventory()
    #         inv.resource_provider_id = provider_id
    #         inv.resource_class_id = resource_class
    #         inv.total = total
    #         inv.reserved = 0
    #         inv.allocation_ratio = 1.0
    #         inv.min_unit = 1
    #         inv.max_unit = 1
    #         inv.step_size = 1
    #         inv.save(context.session)
    #
    #     @sqlalchemy_api.main_context_manager.writer
    #     def create_allocation(context, provider_id, resource_class, used):
    #         alloc = models.Allocation()
    #         alloc.resource_provider_id = provider_id
    #         alloc.resource_class_id = resource_class
    #         alloc.consumer_id = 'xyz'
    #         alloc.used = used
    #         alloc.save(context.session)
    #
    #     # Now add an inventory record for memory and check there is a non-None
    #     # value for the inv_memory_mb field. Don't yet add an allocation record
    #     # for RAM_MB yet so ensure inv_memory_mb_used remains None.
    #     rp_id = create_resource_provider(self.ctxt)
    #     create_inventory(self.ctxt, rp_id, RAM_MB, 4096)
    #     nodes = db.compute_node_get_all(self.ctxt)
    #     self.assertEqual(1, len(nodes))
    #     node = nodes[0]
    #     self.assertEqual(4096, node['inv_memory_mb'])
    #     self.assertIsNone(node['inv_memory_mb_used'])
    #
    #     # Now add an allocation record for an instance consuming some memory
    #     # and check there is a non-None value for the inv_memory_mb_used field.
    #     create_allocation(self.ctxt, rp_id, RAM_MB, 64)
    #     nodes = db.compute_node_get_all(self.ctxt)
    #     self.assertEqual(1, len(nodes))
    #     node = nodes[0]
    #     self.assertEqual(4096, node['inv_memory_mb'])
    #     self.assertEqual(64, node['inv_memory_mb_used'])
    #
    #     # Because of the complex join conditions, it's best to also test the
    #     # other two resource classes and ensure that the joins are correct.
    #     self.assertIsNone(node['inv_vcpus'])
    #     self.assertIsNone(node['inv_vcpus_used'])
    #     self.assertIsNone(node['inv_local_gb'])
    #     self.assertIsNone(node['inv_local_gb_used'])
    #
    #     create_inventory(self.ctxt, rp_id, VCPU, 16)
    #     create_allocation(self.ctxt, rp_id, VCPU, 2)
    #     nodes = db.compute_node_get_all(self.ctxt)
    #     self.assertEqual(1, len(nodes))
    #     node = nodes[0]
    #     self.assertEqual(16, node['inv_vcpus'])
    #     self.assertEqual(2, node['inv_vcpus_used'])
    #     # Check to make sure the other resources stayed the same...
    #     self.assertEqual(4096, node['inv_memory_mb'])
    #     self.assertEqual(64, node['inv_memory_mb_used'])
    #
    #     create_inventory(self.ctxt, rp_id, DISK_GB, 100)
    #     create_allocation(self.ctxt, rp_id, DISK_GB, 20)
    #     nodes = db.compute_node_get_all(self.ctxt)
    #     self.assertEqual(1, len(nodes))
    #     node = nodes[0]
    #     self.assertEqual(100, node['inv_local_gb'])
    #     self.assertEqual(20, node['inv_local_gb_used'])
    #     # Check to make sure the other resources stayed the same...
    #     self.assertEqual(4096, node['inv_memory_mb'])
    #     self.assertEqual(64, node['inv_memory_mb_used'])
    #     self.assertEqual(16, node['inv_vcpus'])
    #     self.assertEqual(2, node['inv_vcpus_used'])

    # def test_compute_node_exec(self):
    #     results = sqlalchemy_api._compute_node_select(self.ctxt)
    #     self.assertIsInstance(results, list)
    #     self.assertEqual(1, len(results))
    #     self.assertIsInstance(results[0], dict)

    def test_compute_node_get_all_deleted_compute_node(self):
        # Create a service and compute node and ensure we can find its stats;
        # delete the service and compute node when done and loop again
        for x in range(2, 5):
            # Create a service
            service_data = self.service_dict.copy()
            service_data['host'] = 'host-%s' % x
            service = db.service_create(self.ctxt, service_data)

            # Create a compute node
            compute_node_data = self.compute_node_dict.copy()
            compute_node_data['service_id'] = service['id']
            compute_node_data['stats'] = jsonutils.dumps(self.stats.copy())
            compute_node_data['hypervisor_hostname'] = 'hypervisor-%s' % x
            node = db.compute_node_create(self.ctxt, compute_node_data)

            # Ensure the "new" compute node is found
            nodes = db.compute_node_get_all(self.ctxt)
            self.assertEqual(2, len(nodes))
            found = None
            for n in nodes:
                if n['id'] == node['id']:
                    found = n
                    break
            self.assertIsNotNone(found)
            # Now ensure the match has stats!
            self.assertNotEqual(jsonutils.loads(found['stats']), {})

            # Now delete the newly-created compute node to ensure the related
            # compute node stats are wiped in a cascaded fashion
            db.compute_node_delete(self.ctxt, node['id'])

            # Clean up the service
            db.service_destroy(self.ctxt, service['id'])

    def test_compute_node_get_all_mult_compute_nodes_one_service_entry(self):
        service_data = self.service_dict.copy()
        service_data['host'] = 'host2'
        service = db.service_create(self.ctxt, service_data)

        existing_node = dict(self.item.items())
        expected = [existing_node]

        for name in ['bm_node1', 'bm_node2']:
            compute_node_data = self.compute_node_dict.copy()
            compute_node_data['service_id'] = service['id']
            compute_node_data['stats'] = jsonutils.dumps(self.stats)
            compute_node_data['hypervisor_hostname'] = name
            node = db.compute_node_create(self.ctxt, compute_node_data)

            node = dict(node)

            expected.append(node)

        result = sorted(db.compute_node_get_all(self.ctxt),
                        key=lambda n: n['hypervisor_hostname'])

        self._assertEqualListsOfObjects(expected, result,
                    ignored_keys=self._ignored_temp_resource_providers_keys +
                                 ['stats'])

    def test_compute_node_get_all_by_host_with_distinct_hosts(self):
        # Create another service with another node
        service2 = self.service_dict.copy()
        service2['host'] = 'host2'
        db.service_create(self.ctxt, service2)
        compute_node_another_host = self.compute_node_dict.copy()
        compute_node_another_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_another_host['hypervisor_hostname'] = 'node_2'
        compute_node_another_host['host'] = 'host2'

        node = db.compute_node_create(self.ctxt, compute_node_another_host)

        result = db.compute_node_get_all_by_host(self.ctxt, 'host1')
        self._assertEqualListsOfObjects([self.item], result,
                ignored_keys=self._ignored_temp_resource_providers_keys)
        result = db.compute_node_get_all_by_host(self.ctxt, 'host2')
        self._assertEqualListsOfObjects([node], result,
                ignored_keys=self._ignored_temp_resource_providers_keys)

    def test_compute_node_get_all_by_host_with_same_host(self):
        # Create another node on top of the same service
        compute_node_same_host = self.compute_node_dict.copy()
        compute_node_same_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_same_host['hypervisor_hostname'] = 'node_3'

        node = db.compute_node_create(self.ctxt, compute_node_same_host)

        expected = [self.item, node]
        result = sorted(db.compute_node_get_all_by_host(
                        self.ctxt, 'host1'),
                        key=lambda n: n['hypervisor_hostname'])

        ignored = ['stats'] + self._ignored_temp_resource_providers_keys
        self._assertEqualListsOfObjects(expected, result,
                                        ignored_keys=ignored)

    def test_compute_node_get_all_by_host_not_found(self):
        self.assertRaises(exception.ComputeHostNotFound,
                          db.compute_node_get_all_by_host, self.ctxt, 'wrong')

    def test_compute_nodes_get_by_service_id_one_result(self):
        expected = [self.item]
        result = db.compute_nodes_get_by_service_id(
            self.ctxt, self.service['id'])

        ignored = ['stats'] + self._ignored_temp_resource_providers_keys
        self._assertEqualListsOfObjects(expected, result,
                                        ignored_keys=ignored)

    def test_compute_nodes_get_by_service_id_multiple_results(self):
        # Create another node on top of the same service
        compute_node_same_host = self.compute_node_dict.copy()
        compute_node_same_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_same_host['hypervisor_hostname'] = 'node_2'

        node = db.compute_node_create(self.ctxt, compute_node_same_host)

        expected = [self.item, node]
        result = sorted(db.compute_nodes_get_by_service_id(
                        self.ctxt, self.service['id']),
                        key=lambda n: n['hypervisor_hostname'])

        ignored = ['stats'] + self._ignored_temp_resource_providers_keys
        self._assertEqualListsOfObjects(expected, result,
                                        ignored_keys=ignored)

    def test_compute_nodes_get_by_service_id_not_found(self):
        self.assertRaises(exception.ServiceNotFound,
                          db.compute_nodes_get_by_service_id, self.ctxt,
                          'fake')

    def test_compute_node_get_by_host_and_nodename(self):
        # Create another node on top of the same service
        compute_node_same_host = self.compute_node_dict.copy()
        compute_node_same_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_same_host['hypervisor_hostname'] = 'node_2'

        node = db.compute_node_create(self.ctxt, compute_node_same_host)

        expected = node
        result = db.compute_node_get_by_host_and_nodename(
            self.ctxt, 'host1', 'node_2')

        self._assertEqualObjects(expected, result,
                    ignored_keys=self._ignored_keys +
                                 self._ignored_temp_resource_providers_keys +
                                 ['stats', 'service'])

    def test_compute_node_get_by_host_and_nodename_not_found(self):
        self.assertRaises(exception.ComputeHostNotFound,
                          db.compute_node_get_by_host_and_nodename,
                          self.ctxt, 'host1', 'wrong')

    def test_compute_node_get(self):
        compute_node_id = self.item['id']
        node = db.compute_node_get(self.ctxt, compute_node_id)
        self._assertEqualObjects(self.compute_node_dict, node,
                ignored_keys=self._ignored_keys +
                             ['stats', 'service'] +
                             self._ignored_temp_resource_providers_keys)
        new_stats = jsonutils.loads(node['stats'])
        self.assertEqual(self.stats, new_stats)

    def test_compute_node_update(self):
        compute_node_id = self.item['id']
        stats = jsonutils.loads(self.item['stats'])
        # change some values:
        stats['num_instances'] = 8
        stats['num_tribbles'] = 1
        values = {
            'vcpus': 4,
            'stats': jsonutils.dumps(stats),
        }
        item_updated = db.compute_node_update(self.ctxt, compute_node_id,
                                              values)
        self.assertEqual(4, item_updated['vcpus'])
        new_stats = jsonutils.loads(item_updated['stats'])
        self.assertEqual(stats, new_stats)
        #NOTE(msimonin) : check if the object has changed in the DB
        compute_node = db.compute_node_get(self.ctxt, compute_node_id)
        self.assertEqual(4, compute_node['vcpus'])



    def test_compute_node_delete(self):
        compute_node_id = self.item['id']
        db.compute_node_delete(self.ctxt, compute_node_id)
        nodes = db.compute_node_get_all(self.ctxt)
        self.assertEqual(len(nodes), 0)

    def test_compute_node_search_by_hypervisor(self):
        nodes_created = []
        new_service = copy.copy(self.service_dict)
        for i in range(3):
            new_service['binary'] += str(i)
            new_service['topic'] += str(i)
            service = db.service_create(self.ctxt, new_service)
            self.compute_node_dict['service_id'] = service['id']
            self.compute_node_dict['hypervisor_hostname'] = 'testhost' + str(i)
            self.compute_node_dict['stats'] = jsonutils.dumps(self.stats)
            node = db.compute_node_create(self.ctxt, self.compute_node_dict)
            nodes_created.append(node)
        nodes = db.compute_node_search_by_hypervisor(self.ctxt, 'host')
        self.assertEqual(3, len(nodes))
        self._assertEqualListsOfObjects(nodes_created, nodes,
                        ignored_keys=self._ignored_keys + ['stats', 'service'])

    def test_compute_node_statistics(self):
        stats = db.compute_node_statistics(self.ctxt)
        self.assertEqual(stats.pop('count'), 1)
        for k, v in stats.items():
            self.assertEqual(v, self.item[k])

    def test_compute_node_statistics_disabled_service(self):
        serv = db.service_get_by_host_and_topic(
            self.ctxt, 'host1', CONF.compute_topic)
        db.service_update(self.ctxt, serv['id'], {'disabled': True})
        stats = db.compute_node_statistics(self.ctxt)
        self.assertEqual(stats.pop('count'), 0)

    def test_compute_node_statistics_with_old_service_id(self):
        # NOTE(sbauza): This test is only for checking backwards compatibility
        # with old versions of compute_nodes not providing host column.
        # This test could be removed once we are sure that all compute nodes
        # are populating the host field thanks to the ResourceTracker

        service2 = self.service_dict.copy()
        service2['host'] = 'host2'
        db_service2 = db.service_create(self.ctxt, service2)
        compute_node_old_host = self.compute_node_dict.copy()
        compute_node_old_host['stats'] = jsonutils.dumps(self.stats)
        compute_node_old_host['hypervisor_hostname'] = 'node_2'
        compute_node_old_host['service_id'] = db_service2['id']
        compute_node_old_host.pop('host')

        db.compute_node_create(self.ctxt, compute_node_old_host)
        stats = db.compute_node_statistics(self.ctxt)
        self.assertEqual(2, stats.pop('count'))

    def test_compute_node_statistics_with_other_service(self):
        other_service = self.service_dict.copy()
        other_service['topic'] = 'fake-topic'
        other_service['binary'] = 'nova-fake'
        db.service_create(self.ctxt, other_service)

        stats = db.compute_node_statistics(self.ctxt)
        data = {'count': 1,
                'vcpus_used': 0,
                'local_gb_used': 0,
                'memory_mb': 1024,
                'current_workload': 0,
                'vcpus': 2,
                'running_vms': 0,
                'free_disk_gb': 2048,
                'disk_available_least': 100,
                'local_gb': 2048,
                'free_ram_mb': 1024,
                'memory_mb_used': 0}
        for key, value in six.iteritems(data):
            self.assertEqual(value, stats.pop(key))

    def test_compute_node_not_found(self):
        self.assertRaises(exception.ComputeHostNotFound, db.compute_node_get,
                          self.ctxt, 100500)

    def test_compute_node_update_always_updates_updated_at(self):
        item_updated = db.compute_node_update(self.ctxt,
                self.item['id'], {})
        self.assertNotEqual(self.item['updated_at'],
                                 item_updated['updated_at'])

    def test_compute_node_update_override_updated_at(self):
        # Update the record once so updated_at is set.
        first = db.compute_node_update(self.ctxt, self.item['id'],
                                       {'free_ram_mb': '12'})
        self.assertIsNotNone(first['updated_at'])

        # Update a second time. Make sure that the updated_at value we send
        # is overridden.
        second = db.compute_node_update(self.ctxt, self.item['id'],
                                        {'updated_at': first.updated_at,
                                         'free_ram_mb': '13'})
        self.assertNotEqual(first['updated_at'], second['updated_at'])

    def test_service_destroy_with_compute_node(self):
        db.service_destroy(self.ctxt, self.service['id'])
        self.assertRaises(exception.ComputeHostNotFound,
                          db.compute_node_get_model, self.ctxt,
                          self.item['id'])

    def test_service_destroy_with_old_compute_node(self):
        # NOTE(sbauza): This test is only for checking backwards compatibility
        # with old versions of compute_nodes not providing host column.
        # This test could be removed once we are sure that all compute nodes
        # are populating the host field thanks to the ResourceTracker
        compute_node_old_host_dict = self.compute_node_dict.copy()
        compute_node_old_host_dict.pop('host')
        item_old = db.compute_node_create(self.ctxt,
                                          compute_node_old_host_dict)

        db.service_destroy(self.ctxt, self.service['id'])
        self.assertRaises(exception.ComputeHostNotFound,
                          db.compute_node_get_model, self.ctxt,
                          item_old['id'])

    @mock.patch("nova.db.discovery.api.compute_node_get_model")
    def test_dbapi_compute_node_get_model(self, mock_get_model):
        cid = self.item["id"]
        db.api.compute_node_get_model(self.ctxt, cid)
        mock_get_model.assert_called_once_with(self.ctxt, cid)

    @mock.patch("nova.db.discovery.api.model_query")
    def test_compute_node_get_model(self, mock_model_query):

        class FakeFiltered(object):
            def first(self):
                return mock.sentinel.first

        fake_filtered_cn = FakeFiltered()

        class FakeModelQuery(object):
            def filter_by(self, id):
                return fake_filtered_cn

        mock_model_query.return_value = FakeModelQuery()
        result = discovery_api.compute_node_get_model(self.ctxt,
                                                       self.item["id"])
        self.assertEqual(result, mock.sentinel.first)
        mock_model_query.assert_called_once_with(self.ctxt, models.ComputeNode)

class FixedIPTestCase(BaseInstanceTypeTestCase):
    def _timeout_test(self, ctxt, timeout, multi_host):
        instance = db.instance_create(ctxt, dict(host='foo'))
        net = db.network_create_safe(ctxt, dict(multi_host=multi_host,
                                                host='bar'))
        old = timeout - datetime.timedelta(seconds=5)
        new = timeout + datetime.timedelta(seconds=5)
        # should deallocate
        db.fixed_ip_create(ctxt, dict(allocated=False,
                                      instance_uuid=instance['uuid'],
                                      network_id=net['id'],
                                      updated_at=old))
        # still allocated
        db.fixed_ip_create(ctxt, dict(allocated=True,
                                      instance_uuid=instance['uuid'],
                                      network_id=net['id'],
                                      updated_at=old))
        # wrong network
        db.fixed_ip_create(ctxt, dict(allocated=False,
                                      instance_uuid=instance['uuid'],
                                      network_id=None,
                                      updated_at=old))
        # too new
        db.fixed_ip_create(ctxt, dict(allocated=False,
                                      instance_uuid=instance['uuid'],
                                      network_id=None,
                                      updated_at=new))

    def mock_db_query_first_to_raise_data_error_exception(self):
        self.mox.StubOutWithMock(Query, 'first')
        Query.first().AndRaise(db_exc.DBError())
        self.mox.ReplayAll()

    def test_fixed_ip_disassociate_all_by_timeout_single_host(self):
        now = timeutils.utcnow()
        self._timeout_test(self.ctxt, now, False)
        result = db.fixed_ip_disassociate_all_by_timeout(self.ctxt, 'foo', now)
        self.assertEqual(result, 0)
        result = db.fixed_ip_disassociate_all_by_timeout(self.ctxt, 'bar', now)
        self.assertEqual(result, 1)

    def test_fixed_ip_disassociate_all_by_timeout_multi_host(self):
        now = timeutils.utcnow()
        self._timeout_test(self.ctxt, now, True)
        result = db.fixed_ip_disassociate_all_by_timeout(self.ctxt, 'foo', now)
        self.assertEqual(result, 1)
        result = db.fixed_ip_disassociate_all_by_timeout(self.ctxt, 'bar', now)
        self.assertEqual(result, 0)

    def test_fixed_ip_get_by_floating_address(self):
        fixed_ip = db.fixed_ip_create(self.ctxt, {'address': '192.168.0.2'})
        values = {'address': '8.7.6.5',
                  'fixed_ip_id': fixed_ip['id']}
        floating = db.floating_ip_create(self.ctxt, values)['address']
        fixed_ip_ref = db.fixed_ip_get_by_floating_address(self.ctxt, floating)
        self._assertEqualObjects(fixed_ip, fixed_ip_ref)

    def test_fixed_ip_get_by_host(self):
        host_ips = {
            'host1': ['1.1.1.1', '1.1.1.2', '1.1.1.3'],
            'host2': ['1.1.1.4', '1.1.1.5'],
            'host3': ['1.1.1.6']
        }

        for host, ips in host_ips.items():
            for ip in ips:
                instance_uuid = self._create_instance(host=host)
                db.fixed_ip_create(self.ctxt, {'address': ip})
                db.fixed_ip_associate(self.ctxt, ip, instance_uuid)

        for host, ips in host_ips.items():
            ips_on_host = [x['address']
                           for x in db.fixed_ip_get_by_host(self.ctxt, host)]
            self._assertEqualListsOfPrimitivesAsSets(ips_on_host, ips)

    def test_fixed_ip_get_by_network_host_not_found_exception(self):
        self.assertRaises(
            exception.FixedIpNotFoundForNetworkHost,
            db.fixed_ip_get_by_network_host,
            self.ctxt, 1, 'ignore')

    def test_fixed_ip_get_by_network_host_fixed_ip_found(self):
        db.fixed_ip_create(self.ctxt, dict(network_id=1, host='host'))

        fip = db.fixed_ip_get_by_network_host(self.ctxt, 1, 'host')

        self.assertEqual(1, fip['network_id'])
        self.assertEqual('host', fip['host'])

    def _create_instance(self, **kwargs):
        instance = db.instance_create(self.ctxt, kwargs)
        return instance['uuid']

    def test_fixed_ip_get_by_instance_fixed_ip_found(self):
        instance_uuid = self._create_instance()

        FIXED_IP_ADDRESS = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS))

        ips_list = db.fixed_ip_get_by_instance(self.ctxt, instance_uuid)
        self._assertEqualListsOfPrimitivesAsSets([FIXED_IP_ADDRESS],
                                                 [ips_list[0].address])

    def test_fixed_ip_get_by_instance_multiple_fixed_ips_found(self):
        instance_uuid = self._create_instance()

        FIXED_IP_ADDRESS_1 = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS_1))
        FIXED_IP_ADDRESS_2 = '192.168.1.6'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS_2))

        ips_list = db.fixed_ip_get_by_instance(self.ctxt, instance_uuid)
        self._assertEqualListsOfPrimitivesAsSets(
            [FIXED_IP_ADDRESS_1, FIXED_IP_ADDRESS_2],
            [ips_list[0].address, ips_list[1].address])

    def test_fixed_ip_get_by_instance_inappropriate_ignored(self):
        instance_uuid = self._create_instance()

        FIXED_IP_ADDRESS_1 = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS_1))
        FIXED_IP_ADDRESS_2 = '192.168.1.6'
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=instance_uuid, address=FIXED_IP_ADDRESS_2))

        another_instance = db.instance_create(self.ctxt, {})
        db.fixed_ip_create(self.ctxt, dict(
            instance_uuid=another_instance['uuid'], address="192.168.1.7"))

        ips_list = db.fixed_ip_get_by_instance(self.ctxt, instance_uuid)
        self._assertEqualListsOfPrimitivesAsSets(
            [FIXED_IP_ADDRESS_1, FIXED_IP_ADDRESS_2],
            [ips_list[0].address, ips_list[1].address])

    def test_fixed_ip_get_by_instance_not_found_exception(self):
        instance_uuid = self._create_instance()

        self.assertRaises(exception.FixedIpNotFoundForInstance,
                          db.fixed_ip_get_by_instance,
                          self.ctxt, instance_uuid)

    def test_fixed_ips_by_virtual_interface_fixed_ip_found(self):
        instance_uuid = self._create_instance()

        vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))

        FIXED_IP_ADDRESS = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS))

        ips_list = db.fixed_ips_by_virtual_interface(self.ctxt, vif.id)
        self._assertEqualListsOfPrimitivesAsSets([FIXED_IP_ADDRESS],
                                                 [ips_list[0].address])

    def test_fixed_ips_by_virtual_interface_multiple_fixed_ips_found(self):
        instance_uuid = self._create_instance()

        vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))

        FIXED_IP_ADDRESS_1 = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS_1))
        FIXED_IP_ADDRESS_2 = '192.168.1.6'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS_2))

        ips_list = db.fixed_ips_by_virtual_interface(self.ctxt, vif.id)
        self._assertEqualListsOfPrimitivesAsSets(
            [FIXED_IP_ADDRESS_1, FIXED_IP_ADDRESS_2],
            [ips_list[0].address, ips_list[1].address])

    def test_fixed_ips_by_virtual_interface_inappropriate_ignored(self):
        instance_uuid = self._create_instance()

        vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))

        FIXED_IP_ADDRESS_1 = '192.168.1.5'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS_1))
        FIXED_IP_ADDRESS_2 = '192.168.1.6'
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=vif.id, address=FIXED_IP_ADDRESS_2))

        another_vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))
        db.fixed_ip_create(self.ctxt, dict(
            virtual_interface_id=another_vif.id, address="192.168.1.7"))

        ips_list = db.fixed_ips_by_virtual_interface(self.ctxt, vif.id)
        self._assertEqualListsOfPrimitivesAsSets(
            [FIXED_IP_ADDRESS_1, FIXED_IP_ADDRESS_2],
            [ips_list[0].address, ips_list[1].address])

    def test_fixed_ips_by_virtual_interface_no_ip_found(self):
        instance_uuid = self._create_instance()

        vif = db.virtual_interface_create(
            self.ctxt, dict(instance_uuid=instance_uuid))

        ips_list = db.fixed_ips_by_virtual_interface(self.ctxt, vif.id)
        self.assertEqual(0, len(ips_list))

    def create_fixed_ip(self, **params):
        default_params = {'address': '192.168.0.1'}
        default_params.update(params)
        return db.fixed_ip_create(self.ctxt, default_params)['address']

    def test_fixed_ip_associate_fails_if_ip_not_in_network(self):
        instance_uuid = self._create_instance()
        self.assertRaises(exception.FixedIpNotFoundForNetwork,
                          db.fixed_ip_associate,
                          self.ctxt, None, instance_uuid)

    def test_fixed_ip_associate_fails_if_ip_in_use(self):
        instance_uuid = self._create_instance()

        address = self.create_fixed_ip(instance_uuid=instance_uuid)
        self.assertRaises(exception.FixedIpAlreadyInUse,
                          db.fixed_ip_associate,
                          self.ctxt, address, instance_uuid)

    def test_fixed_ip_associate_succeeds(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip(network_id=network['id'])
        db.fixed_ip_associate(self.ctxt, address, instance_uuid,
                              network_id=network['id'])
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)

    def test_fixed_ip_associate_succeeds_and_sets_network(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip()
        db.fixed_ip_associate(self.ctxt, address, instance_uuid,
                              network_id=network['id'])
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)
        self.assertEqual(fixed_ip['network_id'], network['id'])

    def test_fixed_ip_associate_succeeds_retry_on_deadlock(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip()

        def fake_first():
            if mock_first.call_count == 1:
                raise db_exc.DBDeadlock()
            else:
                return objects.Instance(id=1, address=address, reserved=False,
                                        instance_uuid=None, network_id=None)

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        side_effect=fake_first) as mock_first:
            db.fixed_ip_associate(self.ctxt, address, instance_uuid,
                                  network_id=network['id'])
            self.assertEqual(2, mock_first.call_count)

        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)
        self.assertEqual(fixed_ip['network_id'], network['id'])

    def test_fixed_ip_associate_succeeds_retry_on_no_rows_updated(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip()

        def fake_first():
            if mock_first.call_count == 1:
                return objects.Instance(id=2, address=address, reserved=False,
                                        instance_uuid=None, network_id=None)
            else:
                return objects.Instance(id=1, address=address, reserved=False,
                                        instance_uuid=None, network_id=None)

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        side_effect=fake_first) as mock_first:
            db.fixed_ip_associate(self.ctxt, address, instance_uuid,
                                  network_id=network['id'])
            self.assertEqual(2, mock_first.call_count)

        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)
        self.assertEqual(fixed_ip['network_id'], network['id'])

    def test_fixed_ip_associate_succeeds_retry_limit_exceeded(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip()

        def fake_first():
            return objects.Instance(id=2, address=address, reserved=False,
                                    instance_uuid=None, network_id=None)

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        side_effect=fake_first) as mock_first:
            self.assertRaises(exception.FixedIpAssociateFailed,
                              db.fixed_ip_associate, self.ctxt, address,
                              instance_uuid, network_id=network['id'])
            # 5 reties + initial attempt
            self.assertEqual(6, mock_first.call_count)

    def test_fixed_ip_associate_ip_not_in_network_with_no_retries(self):
        instance_uuid = self._create_instance()

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        return_value=None) as mock_first:
            self.assertRaises(exception.FixedIpNotFoundForNetwork,
                              db.fixed_ip_associate,
                              self.ctxt, None, instance_uuid)
            self.assertEqual(1, mock_first.call_count)

    def test_fixed_ip_associate_no_network_id_with_no_retries(self):
        # Tests that trying to associate an instance to a fixed IP on a network
        # but without specifying the network ID during associate will fail.
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})
        address = self.create_fixed_ip(network_id=network['id'])

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        return_value=None) as mock_first:
            self.assertRaises(exception.FixedIpNotFoundForNetwork,
                              db.fixed_ip_associate,
                              self.ctxt, address, instance_uuid)
            self.assertEqual(1, mock_first.call_count)

    def test_fixed_ip_associate_with_vif(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})
        vif = db.virtual_interface_create(self.ctxt, {})
        address = self.create_fixed_ip()

        fixed_ip = db.fixed_ip_associate(self.ctxt, address, instance_uuid,
                                         network_id=network['id'],
                                         virtual_interface_id=vif['id'])

        self.assertTrue(fixed_ip['allocated'])
        self.assertEqual(vif['id'], fixed_ip['virtual_interface_id'])

    def test_fixed_ip_associate_not_allocated_without_vif(self):
        instance_uuid = self._create_instance()
        address = self.create_fixed_ip()

        fixed_ip = db.fixed_ip_associate(self.ctxt, address, instance_uuid)

        self.assertFalse(fixed_ip['allocated'])
        self.assertIsNone(fixed_ip['virtual_interface_id'])

    def test_fixed_ip_associate_pool_invalid_uuid(self):
        instance_uuid = '123'
        self.assertRaises(exception.InvalidUUID, db.fixed_ip_associate_pool,
                          self.ctxt, None, instance_uuid)

    def test_fixed_ip_associate_pool_no_more_fixed_ips(self):
        instance_uuid = self._create_instance()
        self.assertRaises(exception.NoMoreFixedIps, db.fixed_ip_associate_pool,
                          self.ctxt, None, instance_uuid)

    def test_fixed_ip_associate_pool_ignores_leased_addresses(self):
        instance_uuid = self._create_instance()
        params = {'address': '192.168.1.5',
                  'leased': True}
        db.fixed_ip_create(self.ctxt, params)
        self.assertRaises(exception.NoMoreFixedIps, db.fixed_ip_associate_pool,
                          self.ctxt, None, instance_uuid)

    def test_fixed_ip_associate_pool_succeeds(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip(network_id=network['id'])
        db.fixed_ip_associate_pool(self.ctxt, network['id'], instance_uuid)
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)

    def test_fixed_ip_associate_pool_order(self):
        """Test that fixed_ip always uses oldest fixed_ip.

        We should always be using the fixed ip with the oldest
        updated_at.
        """
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})
        self.addCleanup(timeutils.clear_time_override)
        start = timeutils.utcnow()
        for i in range(1, 4):
            now = start - datetime.timedelta(hours=i)
            timeutils.set_time_override(now)
            address = self.create_fixed_ip(
                updated_at=now,
                address='10.1.0.%d' % i,
                network_id=network['id'])
        db.fixed_ip_associate_pool(self.ctxt, network['id'], instance_uuid)
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(fixed_ip['instance_uuid'], instance_uuid)

    def test_fixed_ip_associate_pool_succeeds_fip_ref_network_id_is_none(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        self.create_fixed_ip(network_id=None)
        fixed_ip = db.fixed_ip_associate_pool(self.ctxt,
                                              network['id'], instance_uuid)
        self.assertEqual(instance_uuid, fixed_ip['instance_uuid'])
        self.assertEqual(network['id'], fixed_ip['network_id'])

    def test_fixed_ip_associate_pool_succeeds_retry(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        address = self.create_fixed_ip(network_id=network['id'])

        def fake_first():
            if mock_first.call_count == 1:
                return {'network_id': network['id'], 'address': 'invalid',
                        'instance_uuid': None, 'host': None, 'id': 1}
            else:
                return {'network_id': network['id'], 'address': address,
                        'instance_uuid': None, 'host': None, 'id': 1}

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        side_effect=fake_first) as mock_first:
            db.fixed_ip_associate_pool(self.ctxt, network['id'], instance_uuid)
            self.assertEqual(2, mock_first.call_count)

        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, address)
        self.assertEqual(instance_uuid, fixed_ip['instance_uuid'])

    def test_fixed_ip_associate_pool_retry_limit_exceeded(self):
        instance_uuid = self._create_instance()
        network = db.network_create_safe(self.ctxt, {})

        self.create_fixed_ip(network_id=network['id'])

        def fake_first():
            return {'network_id': network['id'], 'address': 'invalid',
                    'instance_uuid': None, 'host': None, 'id': 1}

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        side_effect=fake_first) as mock_first:
            self.assertRaises(exception.FixedIpAssociateFailed,
                              db.fixed_ip_associate_pool, self.ctxt,
                              network['id'], instance_uuid)
            # 5 retries + initial attempt
            self.assertEqual(6, mock_first.call_count)

    def test_fixed_ip_create_same_address(self):
        address = '192.168.1.5'
        params = {'address': address}
        db.fixed_ip_create(self.ctxt, params)
        self.assertRaises(exception.FixedIpExists, db.fixed_ip_create,
                          self.ctxt, params)

    def test_fixed_ip_create_success(self):
        instance_uuid = self._create_instance()
        network_id = db.network_create_safe(self.ctxt, {})['id']
        param = {
            'reserved': False,
            'deleted': 0,
            'leased': False,
            'host': '127.0.0.1',
            'address': '192.168.1.5',
            'allocated': False,
            'instance_uuid': instance_uuid,
            'network_id': network_id,
            'virtual_interface_id': None
        }

        ignored_keys = ['created_at', 'id', 'deleted_at', 'updated_at']
        fixed_ip_data = db.fixed_ip_create(self.ctxt, param)
        self._assertEqualObjects(param, fixed_ip_data, ignored_keys)

    def test_fixed_ip_bulk_create_same_address(self):
        address_1 = '192.168.1.5'
        address_2 = '192.168.1.6'
        instance_uuid = self._create_instance()
        network_id_1 = db.network_create_safe(self.ctxt, {})['id']
        network_id_2 = db.network_create_safe(self.ctxt, {})['id']
        params = [
            {'reserved': False, 'deleted': 0, 'leased': False,
             'host': '127.0.0.1', 'address': address_2, 'allocated': False,
             'instance_uuid': instance_uuid, 'network_id': network_id_1,
             'virtual_interface_id': None},
            {'reserved': False, 'deleted': 0, 'leased': False,
             'host': '127.0.0.1', 'address': address_1, 'allocated': False,
             'instance_uuid': instance_uuid, 'network_id': network_id_1,
             'virtual_interface_id': None},
            {'reserved': False, 'deleted': 0, 'leased': False,
             'host': 'localhost', 'address': address_2, 'allocated': True,
             'instance_uuid': instance_uuid, 'network_id': network_id_2,
             'virtual_interface_id': None},
        ]

        self.assertRaises(exception.FixedIpExists, db.fixed_ip_bulk_create,
                          self.ctxt, params)
        # In this case the transaction will be rolled back and none of the ips
        # will make it to the database.
        self.assertRaises(exception.FixedIpNotFoundForAddress,
                          db.fixed_ip_get_by_address, self.ctxt, address_1)
        self.assertRaises(exception.FixedIpNotFoundForAddress,
                          db.fixed_ip_get_by_address, self.ctxt, address_2)

    def test_fixed_ip_bulk_create_success(self):
        address_1 = '192.168.1.5'
        address_2 = '192.168.1.6'

        instance_uuid = self._create_instance()
        network_id_1 = db.network_create_safe(self.ctxt, {})['id']
        network_id_2 = db.network_create_safe(self.ctxt, {})['id']
        params = [
            {'reserved': False, 'deleted': 0, 'leased': False,
             'host': '127.0.0.1', 'address': address_1, 'allocated': False,
             'instance_uuid': instance_uuid, 'network_id': network_id_1,
             'virtual_interface_id': None},
            {'reserved': False, 'deleted': 0, 'leased': False,
             'host': 'localhost', 'address': address_2, 'allocated': True,
             'instance_uuid': instance_uuid, 'network_id': network_id_2,
             'virtual_interface_id': None}
        ]

        db.fixed_ip_bulk_create(self.ctxt, params)
        ignored_keys = ['created_at', 'id', 'deleted_at', 'updated_at',
                        'virtual_interface', 'network', 'floating_ips']
        fixed_ip_data = db.fixed_ip_get_by_instance(self.ctxt, instance_uuid)

        # we have no `id` in incoming data so we can not use
        # _assertEqualListsOfObjects to compare incoming data and received
        # objects
        fixed_ip_data = sorted(fixed_ip_data, key=lambda i: i['network_id'])
        params = sorted(params, key=lambda i: i['network_id'])
        for param, ip in zip(params, fixed_ip_data):
            self._assertEqualObjects(param, ip, ignored_keys)

    def test_fixed_ip_disassociate(self):
        address = '192.168.1.5'
        instance_uuid = self._create_instance()
        network_id = db.network_create_safe(self.ctxt, {})['id']
        values = {'address': '192.168.1.5', 'instance_uuid': instance_uuid}
        vif = db.virtual_interface_create(self.ctxt, values)
        param = {
            'reserved': False,
            'deleted': 0,
            'leased': False,
            'host': '127.0.0.1',
            'address': address,
            'allocated': False,
            'instance_uuid': instance_uuid,
            'network_id': network_id,
            'virtual_interface_id': vif['id']
        }
        db.fixed_ip_create(self.ctxt, param)

        db.fixed_ip_disassociate(self.ctxt, address)
        fixed_ip_data = db.fixed_ip_get_by_address(self.ctxt, address)
        ignored_keys = ['created_at', 'id', 'deleted_at',
                        'updated_at', 'instance_uuid',
                        'virtual_interface_id']
        self._assertEqualObjects(param, fixed_ip_data, ignored_keys)
        self.assertIsNone(fixed_ip_data['instance_uuid'])
        self.assertIsNone(fixed_ip_data['virtual_interface_id'])

    def test_fixed_ip_get_not_found_exception(self):
        self.assertRaises(exception.FixedIpNotFound,
                          db.fixed_ip_get, self.ctxt, 0)

    def test_fixed_ip_get_success2(self):
        address = '192.168.1.5'
        instance_uuid = self._create_instance()
        network_id = db.network_create_safe(self.ctxt, {})['id']
        param = {
            'reserved': False,
            'deleted': 0,
            'leased': False,
            'host': '127.0.0.1',
            'address': address,
            'allocated': False,
            'instance_uuid': instance_uuid,
            'network_id': network_id,
            'virtual_interface_id': None
        }
        fixed_ip_id = db.fixed_ip_create(self.ctxt, param)

        self.ctxt.is_admin = False
        self.assertRaises(exception.Forbidden, db.fixed_ip_get,
                          self.ctxt, fixed_ip_id)

    def test_fixed_ip_get_success(self):
        address = '192.168.1.5'
        instance_uuid = self._create_instance()
        network_id = db.network_create_safe(self.ctxt, {})['id']
        param = {
            'reserved': False,
            'deleted': 0,
            'leased': False,
            'host': '127.0.0.1',
            'address': address,
            'allocated': False,
            'instance_uuid': instance_uuid,
            'network_id': network_id,
            'virtual_interface_id': None
        }
        db.fixed_ip_create(self.ctxt, param)

        fixed_ip_id = db.fixed_ip_get_by_address(self.ctxt, address)['id']
        fixed_ip_data = db.fixed_ip_get(self.ctxt, fixed_ip_id)
        ignored_keys = ['created_at', 'id', 'deleted_at', 'updated_at']
        self._assertEqualObjects(param, fixed_ip_data, ignored_keys)

    def test_fixed_ip_get_by_address(self):
        instance_uuid = self._create_instance()
        db.fixed_ip_create(self.ctxt, {'address': '1.2.3.4',
                                       'instance_uuid': instance_uuid,
                                       })
        fixed_ip = db.fixed_ip_get_by_address(self.ctxt, '1.2.3.4',
                                              columns_to_join=['instance'])
        self.assertIn('instance', fixed_ip.__dict__)
        self.assertEqual(instance_uuid, fixed_ip.instance.uuid)

    def test_fixed_ip_update_not_found_for_address(self):
        self.assertRaises(exception.FixedIpNotFoundForAddress,
                          db.fixed_ip_update, self.ctxt,
                          '192.168.1.5', {})

    def test_fixed_ip_update(self):
        instance_uuid_1 = self._create_instance()
        instance_uuid_2 = self._create_instance()
        network_id_1 = db.network_create_safe(self.ctxt, {})['id']
        network_id_2 = db.network_create_safe(self.ctxt, {})['id']
        param_1 = {
            'reserved': True, 'deleted': 0, 'leased': True,
            'host': '192.168.133.1', 'address': '10.0.0.2',
            'allocated': True, 'instance_uuid': instance_uuid_1,
            'network_id': network_id_1, 'virtual_interface_id': '123',
        }

        param_2 = {
            'reserved': False, 'deleted': 0, 'leased': False,
            'host': '127.0.0.1', 'address': '10.0.0.3', 'allocated': False,
            'instance_uuid': instance_uuid_2, 'network_id': network_id_2,
            'virtual_interface_id': None
        }

        ignored_keys = ['created_at', 'id', 'deleted_at', 'updated_at']
        fixed_ip_addr = db.fixed_ip_create(self.ctxt, param_1)['address']
        db.fixed_ip_update(self.ctxt, fixed_ip_addr, param_2)
        fixed_ip_after_update = db.fixed_ip_get_by_address(self.ctxt,
                                                           param_2['address'])
        self._assertEqualObjects(param_2, fixed_ip_after_update, ignored_keys)

class FloatingIpTestCase(DiscoveryTestCase, ModelsObjectComparatorMixin):

    def setUp(self):
        super(FloatingIpTestCase, self).setUp()
        self.ctxt = context.get_admin_context()

    def _get_base_values(self):
        return {
            'address': '1.1.1.1',
            'fixed_ip_id': None,
            'project_id': 'fake_project',
            'host': 'fake_host',
            'auto_assigned': False,
            'pool': 'fake_pool',
            'interface': 'fake_interface',
        }

    def mock_db_query_first_to_raise_data_error_exception(self):
        self.mox.StubOutWithMock(Query, 'first')
        Query.first().AndRaise(db_exc.DBError())
        self.mox.ReplayAll()

    def _create_floating_ip(self, values):
        if not values:
            values = {}
        vals = self._get_base_values()
        vals.update(values)
        return db.floating_ip_create(self.ctxt, vals)

    def test_floating_ip_get(self):
        values = [{'address': '0.0.0.0'}, {'address': '1.1.1.1'}]
        floating_ips = [self._create_floating_ip(val) for val in values]

        for floating_ip in floating_ips:
            real_floating_ip = db.floating_ip_get(self.ctxt, floating_ip['id'])
            self._assertEqualObjects(floating_ip, real_floating_ip,
                                     ignored_keys=['fixed_ip'])

    def test_floating_ip_get_not_found(self):
        self.assertRaises(exception.FloatingIpNotFound,
                          db.floating_ip_get, self.ctxt, 100500)

    def test_floating_ip_get_with_long_id_not_found(self):
        self.mock_db_query_first_to_raise_data_error_exception()
        self.assertRaises(exception.InvalidID,
                          db.floating_ip_get, self.ctxt, 123456789101112)

    def test_floating_ip_get_pools(self):
        values = [
            {'address': '0.0.0.0', 'pool': 'abc'},
            {'address': '1.1.1.1', 'pool': 'abc'},
            {'address': '2.2.2.2', 'pool': 'def'},
            {'address': '3.3.3.3', 'pool': 'ghi'},
        ]
        for val in values:
            self._create_floating_ip(val)
        expected_pools = [{'name': x}
                          for x in set(map(lambda x: x['pool'], values))]
        real_pools = db.floating_ip_get_pools(self.ctxt)
        self._assertEqualListsOfPrimitivesAsSets(real_pools, expected_pools)

    def test_floating_ip_allocate_address(self):
        pools = {
            'pool1': ['0.0.0.0', '1.1.1.1'],
            'pool2': ['2.2.2.2'],
            'pool3': ['3.3.3.3', '4.4.4.4', '5.5.5.5']
        }
        for pool, addresses in pools.items():
            for address in addresses:
                vals = {'pool': pool, 'address': address, 'project_id': None}
                self._create_floating_ip(vals)

        project_id = self._get_base_values()['project_id']
        for pool, addresses in pools.items():
            alloc_addrs = []
            for i in addresses:
                float_addr = db.floating_ip_allocate_address(self.ctxt,
                                                             project_id, pool)
                alloc_addrs.append(float_addr)
            self._assertEqualListsOfPrimitivesAsSets(alloc_addrs, addresses)

    def test_floating_ip_allocate_auto_assigned(self):
        addresses = ['1.1.1.1', '1.1.1.2', '1.1.1.3', '1.1.1.4']

        float_ips = []
        for i in range(0, 2):
            float_ips.append(self._create_floating_ip(
                {"address": addresses[i]}))
        for i in range(2, 4):
            float_ips.append(self._create_floating_ip({"address": addresses[i],
                                                       "auto_assigned": True}))

        for i in range(0, 2):
            float_ip = db.floating_ip_get(self.ctxt, float_ips[i].id)
            self.assertFalse(float_ip.auto_assigned)
        for i in range(2, 4):
            float_ip = db.floating_ip_get(self.ctxt, float_ips[i].id)
            self.assertTrue(float_ip.auto_assigned)

    def test_floating_ip_allocate_address_no_more_floating_ips(self):
        self.assertRaises(exception.NoMoreFloatingIps,
                          db.floating_ip_allocate_address,
                          self.ctxt, 'any_project_id', 'no_such_pool')

    def test_floating_ip_allocate_not_authorized(self):
        ctxt = context.RomeRequestContext(user_id='a', project_id='abc',
                                      is_admin=False)
        self.assertRaises(exception.Forbidden,
                          db.floating_ip_allocate_address,
                          ctxt, 'other_project_id', 'any_pool')

    def test_floating_ip_allocate_address_succeeds_retry(self):
        pool = 'pool0'
        address = '0.0.0.0'
        vals = {'pool': pool, 'address': address, 'project_id': None}
        floating_ip = self._create_floating_ip(vals)

        project_id = self._get_base_values()['project_id']

        def fake_first():
            if mock_first.call_count == 1:
                return {'pool': pool, 'project_id': None, 'fixed_ip_id': None,
                        'address': address, 'id': 'invalid_id'}
            else:
                return {'pool': pool, 'project_id': None, 'fixed_ip_id': None,
                        'address': address, 'id': 1}

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        side_effect=fake_first) as mock_first:
            float_addr = db.floating_ip_allocate_address(self.ctxt,
                                                         project_id, pool)
            self.assertEqual(address, float_addr)
            self.assertEqual(2, mock_first.call_count)

        float_ip = db.floating_ip_get(self.ctxt, floating_ip.id)
        self.assertEqual(project_id, float_ip['project_id'])

    def test_floating_ip_allocate_address_retry_limit_exceeded(self):
        pool = 'pool0'
        address = '0.0.0.0'
        vals = {'pool': pool, 'address': address, 'project_id': None}
        self._create_floating_ip(vals)

        project_id = self._get_base_values()['project_id']

        def fake_first():
            return {'pool': pool, 'project_id': None, 'fixed_ip_id': None,
                    'address': address, 'id': 'invalid_id'}

        with mock.patch('lib.rome.core.orm.query.Query.first',
                        side_effect=fake_first) as mock_first:
            self.assertRaises(exception.FloatingIpAllocateFailed,
                              db.floating_ip_allocate_address, self.ctxt,
                              project_id, pool)
            # 5 retries + initial attempt
            self.assertEqual(6, mock_first.call_count)

    def test_floating_ip_allocate_address_no_more_ips_with_no_retries(self):
        with mock.patch('lib.rome.core.orm.query.Query.first',
                        return_value=None) as mock_first:
            self.assertRaises(exception.NoMoreFloatingIps,
                              db.floating_ip_allocate_address,
                              self.ctxt, 'any_project_id', 'no_such_pool')
            self.assertEqual(1, mock_first.call_count)

    def _get_existing_ips(self):
        return [ip['address'] for ip in db.floating_ip_get_all(self.ctxt)]

    def test_floating_ip_bulk_create(self):
        expected_ips = ['1.1.1.1', '1.1.1.2', '1.1.1.3', '1.1.1.4']
        result = db.floating_ip_bulk_create(self.ctxt,
                                   [{'address': x} for x in expected_ips],
                                   want_result=False)
        self.assertIsNone(result)
        self._assertEqualListsOfPrimitivesAsSets(self._get_existing_ips(),
                                                 expected_ips)

    def test_floating_ip_bulk_create_duplicate(self):
        ips = ['1.1.1.1', '1.1.1.2', '1.1.1.3', '1.1.1.4']
        prepare_ips = lambda x: {'address': x}

        result = db.floating_ip_bulk_create(self.ctxt,
                                            list(map(prepare_ips, ips)))
        self.assertEqual(ips, [ip.address for ip in result])
        self.assertRaises(exception.FloatingIpExists,
                          db.floating_ip_bulk_create,
                          self.ctxt,
                          list(map(prepare_ips, ['1.1.1.5', '1.1.1.4'])),
                          want_result=False)
        self.assertRaises(exception.FloatingIpNotFoundForAddress,
                          db.floating_ip_get_by_address,
                          self.ctxt, '1.1.1.5')

    def test_floating_ip_bulk_destroy(self):
        ips_for_delete = []
        ips_for_non_delete = []

        def create_ips(i, j):
            return [{'address': '1.1.%s.%s' % (i, k)} for k in range(1, j + 1)]

        # NOTE(boris-42): Create more than 256 ip to check that
        #                 _ip_range_splitter works properly.
        for i in range(1, 3):
            ips_for_delete.extend(create_ips(i, 255))
        ips_for_non_delete.extend(create_ips(3, 255))

        result = db.floating_ip_bulk_create(self.ctxt,
                                   ips_for_delete + ips_for_non_delete,
                                   want_result=False)
        self.assertIsNone(result)

        non_bulk_ips_for_delete = create_ips(4, 3)
        non_bulk_ips_for_non_delete = create_ips(5, 3)
        non_bulk_ips = non_bulk_ips_for_delete + non_bulk_ips_for_non_delete
        project_id = 'fake_project'
        reservations = quota.QUOTAS.reserve(self.ctxt,
                                      floating_ips=len(non_bulk_ips),
                                      project_id=project_id)
        for dct in non_bulk_ips:
            self._create_floating_ip(dct)
        quota.QUOTAS.commit(self.ctxt, reservations, project_id=project_id)
        self.assertEqual(db.quota_usage_get_all_by_project(
                            self.ctxt, project_id),
                            {'project_id': project_id,
                             'floating_ips': {'in_use': 6, 'reserved': 0}})
        ips_for_delete.extend(non_bulk_ips_for_delete)
        ips_for_non_delete.extend(non_bulk_ips_for_non_delete)

        db.floating_ip_bulk_destroy(self.ctxt, ips_for_delete)

        expected_addresses = [x['address'] for x in ips_for_non_delete]
        self._assertEqualListsOfPrimitivesAsSets(self._get_existing_ips(),
                                                 expected_addresses)
        self.assertEqual(db.quota_usage_get_all_by_project(
                            self.ctxt, project_id),
                            {'project_id': project_id,
                             'floating_ips': {'in_use': 3, 'reserved': 0}})

    def test_floating_ip_create(self):
        floating_ip = self._create_floating_ip({})
        ignored_keys = ['id', 'deleted', 'deleted_at', 'updated_at',
                        'created_at']

        self.assertIsNotNone(floating_ip['id'])
        self._assertEqualObjects(floating_ip, self._get_base_values(),
                                 ignored_keys)

    def test_floating_ip_create_duplicate(self):
        self._create_floating_ip({})
        self.assertRaises(exception.FloatingIpExists,
                          self._create_floating_ip, {})

    def _create_fixed_ip(self, params):
        default_params = {'address': '192.168.0.1'}
        default_params.update(params)
        return db.fixed_ip_create(self.ctxt, default_params)['address']

    def test_floating_ip_fixed_ip_associate(self):
        float_addresses = ['1.1.1.1', '1.1.1.2', '1.1.1.3']
        fixed_addresses = ['2.2.2.1', '2.2.2.2', '2.2.2.3']

        project_id = self.ctxt.project_id
        float_ips = [self._create_floating_ip({'address': address,
                                               'project_id': project_id})
                        for address in float_addresses]
        fixed_addrs = [self._create_fixed_ip({'address': address})
                        for address in fixed_addresses]

        for float_ip, fixed_addr in zip(float_ips, fixed_addrs):
            fixed_ip = db.floating_ip_fixed_ip_associate(self.ctxt,
                                                         float_ip.address,
                                                         fixed_addr, 'host')
            self.assertEqual(fixed_ip.address, fixed_addr)

            updated_float_ip = db.floating_ip_get(self.ctxt, float_ip.id)
            self.assertEqual(fixed_ip.id, updated_float_ip.fixed_ip_id)
            self.assertEqual('host', updated_float_ip.host)

        fixed_ip = db.floating_ip_fixed_ip_associate(self.ctxt,
                                                     float_addresses[0],
                                                     fixed_addresses[0],
                                                     'host')
        self.assertEqual(fixed_ip.address, fixed_addresses[0])

    def test_floating_ip_fixed_ip_associate_float_ip_not_found(self):
        self.assertRaises(exception.FixedIpNotFoundForAddress,
                          db.floating_ip_fixed_ip_associate,
                          self.ctxt, '10.10.10.10', 'some', 'some')

    def test_floating_ip_associate_failed(self):
        fixed_ip = self._create_fixed_ip({'address': '7.7.7.7'})
        self.assertRaises(exception.FloatingIpAssociateFailed,
                          db.floating_ip_fixed_ip_associate,
                          self.ctxt, '10.10.10.10', fixed_ip, 'some')

    def test_floating_ip_deallocate(self):
        values = {'address': '1.1.1.1', 'project_id': 'fake', 'host': 'fake'}
        float_ip = self._create_floating_ip(values)
        rows_updated = db.floating_ip_deallocate(self.ctxt, float_ip.address)
        self.assertEqual(1, rows_updated)

        updated_float_ip = db.floating_ip_get(self.ctxt, float_ip.id)
        self.assertIsNone(updated_float_ip.project_id)
        self.assertIsNone(updated_float_ip.host)
        self.assertFalse(updated_float_ip.auto_assigned)

    def test_floating_ip_deallocate_address_not_found(self):
        self.assertEqual(0, db.floating_ip_deallocate(self.ctxt, '2.2.2.2'))

    def test_floating_ip_deallocate_address_associated_ip(self):
        float_address = '1.1.1.1'
        fixed_address = '2.2.2.1'

        project_id = self.ctxt.project_id
        float_ip = self._create_floating_ip({'address': float_address,
                                             'project_id': project_id})
        fixed_addr = self._create_fixed_ip({'address': fixed_address})
        db.floating_ip_fixed_ip_associate(self.ctxt, float_ip.address,
                                          fixed_addr, 'host')
        self.assertEqual(0, db.floating_ip_deallocate(self.ctxt,
                                                      float_address))

    def test_floating_ip_destroy(self):
        addresses = ['1.1.1.1', '1.1.1.2', '1.1.1.3']
        float_ips = [self._create_floating_ip({'address': addr})
                        for addr in addresses]

        expected_len = len(addresses)
        for float_ip in float_ips:
            db.floating_ip_destroy(self.ctxt, float_ip.address)
            self.assertRaises(exception.FloatingIpNotFound,
                              db.floating_ip_get, self.ctxt, float_ip.id)
            expected_len -= 1
            if expected_len > 0:
                self.assertEqual(expected_len,
                                 len(db.floating_ip_get_all(self.ctxt)))
            else:
                self.assertRaises(exception.NoFloatingIpsDefined,
                                  db.floating_ip_get_all, self.ctxt)

    def test_floating_ip_disassociate(self):
        float_addresses = ['1.1.1.1', '1.1.1.2', '1.1.1.3']
        fixed_addresses = ['2.2.2.1', '2.2.2.2', '2.2.2.3']

        project_id = self.ctxt.project_id
        float_ips = [self._create_floating_ip({'address': address,
                                               'project_id': project_id})
                        for address in float_addresses]
        fixed_addrs = [self._create_fixed_ip({'address': address})
                        for address in fixed_addresses]

        for float_ip, fixed_addr in zip(float_ips, fixed_addrs):
            db.floating_ip_fixed_ip_associate(self.ctxt,
                                              float_ip.address,
                                              fixed_addr, 'host')

        for float_ip, fixed_addr in zip(float_ips, fixed_addrs):
            fixed = db.floating_ip_disassociate(self.ctxt, float_ip.address)
            self.assertEqual(fixed.address, fixed_addr)
            updated_float_ip = db.floating_ip_get(self.ctxt, float_ip.id)
            self.assertIsNone(updated_float_ip.fixed_ip_id)
            self.assertIsNone(updated_float_ip.host)

    def test_floating_ip_disassociate_not_found(self):
        self.assertRaises(exception.FloatingIpNotFoundForAddress,
                          db.floating_ip_disassociate, self.ctxt,
                          '11.11.11.11')

    def test_floating_ip_get_all(self):
        addresses = ['1.1.1.1', '1.1.1.2', '1.1.1.3']
        float_ips = [self._create_floating_ip({'address': addr})
                        for addr in addresses]
        self._assertEqualListsOfObjects(float_ips,
                                        db.floating_ip_get_all(self.ctxt),
                                        ignored_keys="fixed_ip")

    def test_floating_ip_get_all_associated(self):
        instance = db.instance_create(self.ctxt, {'uuid': 'fake'})
        project_id = self.ctxt.project_id
        float_ip = self._create_floating_ip({'address': '1.1.1.1',
                                             'project_id': project_id})
        fixed_ip = self._create_fixed_ip({'address': '2.2.2.2',
                                          'instance_uuid': instance.uuid})
        db.floating_ip_fixed_ip_associate(self.ctxt,
                                          float_ip.address,
                                          fixed_ip,
                                          'host')
        float_ips = db.floating_ip_get_all(self.ctxt)
        self.assertEqual(1, len(float_ips))
        self.assertEqual(float_ip.address, float_ips[0].address)
        self.assertEqual(fixed_ip, float_ips[0].fixed_ip.address)
        self.assertEqual(instance.uuid, float_ips[0].fixed_ip.instance_uuid)

    def test_floating_ip_get_all_not_found(self):
        self.assertRaises(exception.NoFloatingIpsDefined,
                          db.floating_ip_get_all, self.ctxt)

    def test_floating_ip_get_all_by_host(self):
        hosts = {
            'host1': ['1.1.1.1', '1.1.1.2'],
            'host2': ['2.1.1.1', '2.1.1.2'],
            'host3': ['3.1.1.1', '3.1.1.2', '3.1.1.3']
        }

        hosts_with_float_ips = {}
        for host, addresses in hosts.items():
            hosts_with_float_ips[host] = []
            for address in addresses:
                float_ip = self._create_floating_ip({'host': host,
                                                     'address': address})
                hosts_with_float_ips[host].append(float_ip)

        for host, float_ips in hosts_with_float_ips.items():
            real_float_ips = db.floating_ip_get_all_by_host(self.ctxt, host)
            self._assertEqualListsOfObjects(float_ips, real_float_ips,
                                            ignored_keys="fixed_ip")

    def test_floating_ip_get_all_by_host_not_found(self):
        self.assertRaises(exception.FloatingIpNotFoundForHost,
                          db.floating_ip_get_all_by_host,
                          self.ctxt, 'non_exists_host')

    def test_floating_ip_get_all_by_project(self):
        projects = {
            'pr1': ['1.1.1.1', '1.1.1.2'],
            'pr2': ['2.1.1.1', '2.1.1.2'],
            'pr3': ['3.1.1.1', '3.1.1.2', '3.1.1.3']
        }

        projects_with_float_ips = {}
        for project_id, addresses in projects.items():
            projects_with_float_ips[project_id] = []
            for address in addresses:
                float_ip = self._create_floating_ip({'project_id': project_id,
                                                     'address': address})
                projects_with_float_ips[project_id].append(float_ip)

        for project_id, float_ips in projects_with_float_ips.items():
            real_float_ips = db.floating_ip_get_all_by_project(self.ctxt,
                                                               project_id)
            self._assertEqualListsOfObjects(float_ips, real_float_ips,
                                            ignored_keys='fixed_ip')

    def test_floating_ip_get_all_by_project_not_authorized(self):
        ctxt = context.RomeRequestContext(user_id='a', project_id='abc',
                                      is_admin=False)
        self.assertRaises(exception.Forbidden,
                          db.floating_ip_get_all_by_project,
                          ctxt, 'other_project')

    def test_floating_ip_get_by_address(self):
        addresses = ['1.1.1.1', '1.1.1.2', '1.1.1.3']
        float_ips = [self._create_floating_ip({'address': addr})
                        for addr in addresses]

        for float_ip in float_ips:
            real_float_ip = db.floating_ip_get_by_address(self.ctxt,
                                                          float_ip.address)
            self._assertEqualObjects(float_ip, real_float_ip,
                                     ignored_keys='fixed_ip')

    def test_floating_ip_get_by_address_not_found(self):
        self.assertRaises(exception.FloatingIpNotFoundForAddress,
                          db.floating_ip_get_by_address,
                          self.ctxt, '20.20.20.20')

    def test_floating_ip_get_by_invalid_address(self):
        self.mock_db_query_first_to_raise_data_error_exception()
        self.assertRaises(exception.InvalidIpAddressError,
                          db.floating_ip_get_by_address,
                          self.ctxt, 'non_exists_host')

    def test_floating_ip_get_by_fixed_address(self):
        fixed_float = [
            ('1.1.1.1', '2.2.2.1'),
            ('1.1.1.2', '2.2.2.2'),
            ('1.1.1.3', '2.2.2.3')
        ]

        for fixed_addr, float_addr in fixed_float:
            project_id = self.ctxt.project_id
            self._create_floating_ip({'address': float_addr,
                                      'project_id': project_id})
            self._create_fixed_ip({'address': fixed_addr})
            db.floating_ip_fixed_ip_associate(self.ctxt, float_addr,
                                              fixed_addr, 'some_host')

        for fixed_addr, float_addr in fixed_float:
            float_ip = db.floating_ip_get_by_fixed_address(self.ctxt,
                                                           fixed_addr)
            self.assertEqual(float_addr, float_ip[0]['address'])

    def test_floating_ip_get_by_fixed_ip_id(self):
        fixed_float = [
            ('1.1.1.1', '2.2.2.1'),
            ('1.1.1.2', '2.2.2.2'),
            ('1.1.1.3', '2.2.2.3')
        ]

        for fixed_addr, float_addr in fixed_float:
            project_id = self.ctxt.project_id
            self._create_floating_ip({'address': float_addr,
                                      'project_id': project_id})
            self._create_fixed_ip({'address': fixed_addr})
            db.floating_ip_fixed_ip_associate(self.ctxt, float_addr,
                                              fixed_addr, 'some_host')

        for fixed_addr, float_addr in fixed_float:
            fixed_ip = db.fixed_ip_get_by_address(self.ctxt, fixed_addr)
            float_ip = db.floating_ip_get_by_fixed_ip_id(self.ctxt,
                                                         fixed_ip['id'])
            self.assertEqual(float_addr, float_ip[0]['address'])

    def test_floating_ip_update(self):
        float_ip = self._create_floating_ip({})

        values = {
            'project_id': 'some_pr',
            'host': 'some_host',
            'auto_assigned': True,
            'interface': 'some_interface',
            'pool': 'some_pool'
        }
        floating_ref = db.floating_ip_update(self.ctxt, float_ip['address'],
                                             values)
        self.assertIsNotNone(floating_ref)
        updated_float_ip = db.floating_ip_get(self.ctxt, float_ip['id'])
        self._assertEqualObjects(updated_float_ip, values,
                                 ignored_keys=['id', 'address', 'updated_at',
                                               'deleted_at', 'created_at',
                                               'deleted', 'fixed_ip_id',
                                               'fixed_ip'])

    def test_floating_ip_update_to_duplicate(self):
        float_ip1 = self._create_floating_ip({'address': '1.1.1.1'})
        float_ip2 = self._create_floating_ip({'address': '1.1.1.2'})

        self.assertRaises(exception.FloatingIpExists,
                          db.floating_ip_update,
                          self.ctxt, float_ip2['address'],
                          {'address': float_ip1['address']})


class TestInstanceInfoCache(DiscoveryTestCase):
    def setUp(self):
        super(TestInstanceInfoCache, self).setUp()
        user_id = 'fake'
        project_id = 'fake'
        self.context = context.get_admin_context()


    def test_instance_info_cache_get(self):
        instance = db.instance_create(self.context, {})
        network_info = 'net'
        db.instance_info_cache_update(self.context, instance.uuid,
                                      {'network_info': network_info})
        info_cache = db.instance_info_cache_get(self.context, instance.uuid)
        self.assertEqual(network_info, info_cache.network_info)

    def test_instance_info_cache_update(self):
        instance = db.instance_create(self.context, {})

        network_info1 = 'net1'
        db.instance_info_cache_update(self.context, instance.uuid,
                                      {'network_info': network_info1})
        info_cache = db.instance_info_cache_get(self.context, instance.uuid)
        self.assertEqual(network_info1, info_cache.network_info)

        network_info2 = 'net2'
        db.instance_info_cache_update(self.context, instance.uuid,
                                      {'network_info': network_info2})
        info_cache = db.instance_info_cache_get(self.context, instance.uuid)
        self.assertEqual(network_info2, info_cache.network_info)

    def test_instance_info_cache_delete(self):
        instance = db.instance_create(self.context, {})
        network_info = 'net'
        db.instance_info_cache_update(self.context, instance.uuid,
                                      {'network_info': network_info})
        info_cache = db.instance_info_cache_get(self.context, instance.uuid)
        self.assertEqual(network_info, info_cache.network_info)
        db.instance_info_cache_delete(self.context, instance.uuid)
        info_cache = db.instance_info_cache_get(self.context, instance.uuid)
        self.assertIsNone(info_cache)

    def test_instance_info_cache_update_duplicate(self):
        instance1 = db.instance_create(self.context, {})
        instance2 = db.instance_create(self.context, {})

        network_info1 = 'net1'
        db.instance_info_cache_update(self.context, instance1.uuid,
                                      {'network_info': network_info1})
        network_info2 = 'net2'
        db.instance_info_cache_update(self.context, instance2.uuid,
                                      {'network_info': network_info2})

        # updating of instance_uuid causes unique constraint failure,
        # using of savepoint helps to continue working with existing session
        # after DB errors, so exception was successfully handled
        db.instance_info_cache_update(self.context, instance2.uuid,
                                      {'instance_uuid': instance1.uuid})

        info_cache1 = db.instance_info_cache_get(self.context, instance1.uuid)
        self.assertEqual(network_info1, info_cache1.network_info)
        info_cache2 = db.instance_info_cache_get(self.context, instance2.uuid)
        self.assertEqual(network_info2, info_cache2.network_info)

    def test_instance_info_cache_create_using_update(self):
        network_info = 'net'
        instance_uuid = uuidutils.generate_uuid()
        db.instance_info_cache_update(self.context, instance_uuid,
                                      {'network_info': network_info})
        info_cache = db.instance_info_cache_get(self.context, instance_uuid)
        self.assertEqual(network_info, info_cache.network_info)
        self.assertEqual(instance_uuid, info_cache.instance_uuid)
