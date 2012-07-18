// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <tr1/memory>
#include <map>
#include <set>
#include <boost/scoped_ptr.hpp>

#include "test/ObjectMap/KeyValueDBMemory.h"
#include "os/KeyValueDB.h"
#include "os/LevelDBStore.h"
#include <sys/types.h>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "gtest/gtest.h"

using namespace std;

string store_path;

class IteratorsTest : public ::testing::Test
{
public:
  boost::scoped_ptr<KeyValueDB> db;
  boost::scoped_ptr<KeyValueDBMemory> mock;

  virtual void SetUp() {
    assert(!store_path.empty());

    LevelDBStore *db_ptr = new LevelDBStore(store_path);
    assert(!db_ptr->init(std::cerr));
    db.reset(db_ptr);
    mock.reset(new KeyValueDBMemory());
  }

  virtual void TearDown() { }

  ::testing::AssertionResult validate_db_clear() {
    KeyValueDB::WholeSpaceIterator it = db->get_iterator();
    it->seek_to_first();
    while (it->valid()) {
      pair<string,string> k = it->raw_key();
      if (mock->db.count(k)) {
	return ::testing::AssertionFailure()
		<< __func__
		<< " mock db.count() " << mock->db.count(k)
		<< " key(" << k.first << "," << k.second << ")";
      }
      it->next();
    }
    return ::testing::AssertionSuccess();
  }

  ::testing::AssertionResult validate_db_match() {
    KeyValueDB::WholeSpaceIterator it = db->get_iterator();
    it->seek_to_first();
    while (it->valid()) {
      pair<string, string> k = it->raw_key();
      if (!mock->db.count(k)) {
	return ::testing::AssertionFailure()
		<< __func__
		<< " mock db.count() " << mock->db.count(k)
		<< " key(" << k.first << "," << k.second << ")";
      }

      bufferlist it_bl = it->value();
      bufferlist mock_bl = mock->db[k];

      string it_val = _bl_to_str(it_bl);
      string mock_val = _bl_to_str(mock_bl);

      if (it_val != mock_val) {
	return ::testing::AssertionFailure()
		<< __func__
		<< " key(" << k.first << "," << k.second << ")"
		<< " mismatch db value(" << it_val << ")"
		<< " mock value(" << mock_val << ")";
      }
      it->next();
    }
    return ::testing::AssertionSuccess();
  }

  ::testing::AssertionResult validate_iterator(
				string expected_prefix,
				string expected_key,
				KeyValueDB::WholeSpaceIterator it) {
    if (!it->valid()) {
      return ::testing::AssertionFailure()
	      << __func__
	      << " iterator not valid";
    }
    pair<string,string> key = it->raw_key();

    if (expected_prefix != key.first) {
      return ::testing::AssertionFailure()
	      << __func__
	      << " expected prefix '" << expected_prefix << "'"
	      << " got prefix '" << key.first << "'";
    }

    if (expected_key != it->key()) {
      return ::testing::AssertionFailure()
	      << __func__
	      << " expected key '" << expected_key << "'"
	      << " got key '" << it->key() << "'";
    }

    if (it->key() != key.second) {
      return ::testing::AssertionFailure()
	      << __func__
	      << " key '" << it->key() << "'"
	      << " does not match"
	      << " pair key '" << key.second << "'";
    }
    return ::testing::AssertionSuccess();
  }

  void clear(KeyValueDB *db) {
    KeyValueDB::WholeSpaceIterator it = db->get_snapshot_iterator();
    it->seek_to_first();
    KeyValueDB::Transaction t = db->get_transaction();
    while (it->valid()) {
      pair<string,string> k = it->raw_key();
      t->rmkey(k.first, k.second);
      it->next();
    }
    db->submit_transaction_sync(t);
  }

  string _bl_to_str(bufferlist val) {
    string str(val.c_str(), val.length());
    return str;
  }

  bufferlist _gen_val(string key) {
    ostringstream ss;
    ss << "##value##" << key << "##";
    string val = ss.str();
    bufferptr bp(val.c_str(), val.size());
    bufferlist bl;
    bl.append(bp);
    return bl;
  }

  void _output_iterator(KeyValueDB::WholeSpaceIterator it) {
    if (!it->valid()) {
      std::cerr << __func__ << " iterator is not valid; seeking to first"
		<< std::endl;
      it->seek_to_first();
    }
    int i = 0;
    while (it->valid()) {
      std::cerr << __func__
		<< " #" << (++i) << " > key: " << it->key()
		<< " value: " << it->value() << std::endl;
      it->next();
    }
  }
};

class SinglePrefixIteratorsTest : public IteratorsTest
{
public:
  string prefix;

  virtual void SetUp() {
    IteratorsTest::SetUp();

    prefix = "_TEST_";

    clear(db.get());
    ASSERT_TRUE(validate_db_clear());
    clear(mock.get());
    ASSERT_TRUE(validate_db_match());

    KeyValueDB::Transaction t_db = db->get_transaction();
    t_db->set(prefix, "01", _gen_val("01"));
    t_db->set(prefix, "02", _gen_val("02"));
    t_db->set(prefix, "03", _gen_val("03"));

    KeyValueDB::Transaction t_mock = mock->get_transaction();
    t_mock->set(prefix, "01", _gen_val("01"));
    t_mock->set(prefix, "02", _gen_val("02"));
    t_mock->set(prefix, "03", _gen_val("03"));

    db->submit_transaction_sync(t_db);
    mock->submit_transaction_sync(t_mock);

    ASSERT_TRUE(validate_db_match());
  }

  virtual void TearDown() {
    IteratorsTest::TearDown();
  }

  void run_test(KeyValueDB *db,
		KeyValueDB::WholeSpaceIterator it) {

  }
};

TEST_F(SinglePrefixIteratorsTest, WholeSpaceIteratorRmKeys)
{
  /* The LevelDBStore's iterator appears to maintain the same state it had
   * once the iteratio began, even if we remove keys from the store. Thus we
   * don't have any problems with moving the iterator's position.
   *
   * However, the KeyValueDBMemory iterator is unable to cope with such
   * operations, and will segfault once we try to use the iterator's handle
   * after removing the item the handle was supposed to point to.
   */
  KeyValueDB::WholeSpaceIterator db_it = db->get_iterator();
  db_it->seek_to_first();
  ASSERT_TRUE(db_it->valid());

  KeyValueDB::Transaction t = db->get_transaction();
  t->rmkey(prefix, "01");
  t->rmkey(prefix, "02");
  db->submit_transaction_sync(t);

  ASSERT_TRUE(db_it->valid());
  ASSERT_STREQ(db_it->key().c_str(), "01");
  ASSERT_STREQ(_bl_to_str(db_it->value()).c_str(),
	       _bl_to_str(_gen_val("01")).c_str());

  db_it->next();
  ASSERT_TRUE(db_it->valid());
  db_it->next();
  ASSERT_TRUE(db_it->valid());

  ASSERT_STREQ(db_it->key().c_str(), "03");
  ASSERT_STREQ(_bl_to_str(db_it->value()).c_str(),
	       _bl_to_str(_gen_val("03")).c_str());
  /*
   * This will segfault
   *
   * During this test, we will remove keys from the in-memory mock store,
   * while iterating over those same keys. Given the test sequence of
   * operations (which are intended to test just this), the in-memory
   * iterator (the one that iterates over the map in the in-memory
   * implementation) will become NULL and therefore it will eventually
   * segfault. The in-memory implementation of this iterator just doesn't
   * foresees the possibility we are here exploring.
   *
   * Using its snapshot iterator solves this problem.
   */
#if 0
  KeyValueDB::WholeSpaceIterator mock_it = db->get_iterator();
  mock_it->seek_to_first();
  ASSERT_TRUE(mock_it->valid());

  KeyValueDB::Transaction t = db->get_transaction();
  t->rmkey(prefix, "01");
  t->rmkey(prefix, "02");
  db->submit_transaction_sync(t);

  ASSERT_TRUE(mock_it->valid());
  ASSERT_STREQ(mock_it->key().c_str(), "01");
  ASSERT_STREQ(_bl_to_str(mock_it->value()).c_str(),
      _bl_to_str(_gen_val("01")).c_str());

  mock_it->next();
  ASSERT_TRUE(mock_it->valid());
  mock_it->next();
  ASSERT_TRUE(mock_it->valid());

  ASSERT_STREQ(mock_it->key().c_str(), "03");
  ASSERT_STREQ(_bl_to_str(mock_it->value()).c_str(),
      _bl_to_str(_gen_val("03")).c_str());  std::cerr << __func__ << " test KeyValueDBMemory iterator" << std::endl;
  )
  ASSERT_TRUE(validate_db_match());
#endif
};

TEST_F(SinglePrefixIteratorsTest, WholeSpaceSnapshotIteratorRmKeys)
{
  // test leveldb store
  KeyValueDB::WholeSpaceIterator db_it = db->get_snapshot_iterator();
  db_it->seek_to_first();
  ASSERT_TRUE(db_it->valid());

  KeyValueDB::Transaction t_db = db->get_transaction();
  t_db->rmkey(prefix, "01");
  t_db->rmkey(prefix, "02");
  db->submit_transaction_sync(t_db);

  ASSERT_TRUE(db_it->valid());
  ASSERT_STREQ(db_it->key().c_str(), "01");
  ASSERT_STREQ(_bl_to_str(db_it->value()).c_str(),
	       _bl_to_str(_gen_val("01")).c_str());

  db_it->next();
  ASSERT_TRUE(db_it->valid());
  db_it->next();
  ASSERT_TRUE(db_it->valid());

  ASSERT_STREQ(db_it->key().c_str(), "03");
  ASSERT_STREQ(_bl_to_str(db_it->value()).c_str(),
	       _bl_to_str(_gen_val("03")).c_str());

  // test mock store
  KeyValueDB::WholeSpaceIterator mock_it = mock->get_snapshot_iterator();
  mock_it->seek_to_first();
  ASSERT_TRUE(mock_it->valid());

  KeyValueDB::Transaction t_mock = mock->get_transaction();
  t_mock->rmkey(prefix, "01");
  t_mock->rmkey(prefix, "02");
  mock->submit_transaction_sync(t_mock);

  ASSERT_TRUE(mock_it->valid());
  ASSERT_STREQ(mock_it->key().c_str(), "01");
  ASSERT_STREQ(_bl_to_str(mock_it->value()).c_str(),
	       _bl_to_str(_gen_val("01")).c_str());

  mock_it->next();
  ASSERT_TRUE(mock_it->valid());
  mock_it->next();
  ASSERT_TRUE(mock_it->valid());

  ASSERT_STREQ(mock_it->key().c_str(), "03");
  ASSERT_STREQ(_bl_to_str(mock_it->value()).c_str(),
	       _bl_to_str(_gen_val("03")).c_str());

  ASSERT_TRUE(validate_db_match());
};

TEST_F(SinglePrefixIteratorsTest, SnapshotIteratorUpdatesLevelDB)
{
  KeyValueDB::WholeSpaceIterator db_it = db->get_snapshot_iterator();
  KeyValueDB::Transaction db_tx = db->get_transaction();

  db_it->seek_to_first();
  ASSERT_TRUE(validate_iterator(prefix, "01", db_it));

  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix, "02", db_it));
  
  db_tx->set(prefix, "02", _gen_val("###bar###"));
  db->submit_transaction_sync(db_tx);

  ASSERT_TRUE(validate_iterator(prefix, "02", db_it));
  ASSERT_STREQ(_bl_to_str(db_it->value()).c_str(),
	       _bl_to_str(_gen_val("02")).c_str());
}

TEST_F(SinglePrefixIteratorsTest, SnapshotIteratorUpdatesMockDB)
{
  KeyValueDB::WholeSpaceIterator mock_it = mock->get_snapshot_iterator();
  KeyValueDB::Transaction mock_tx = mock->get_transaction();

  mock_it->seek_to_first();
  ASSERT_TRUE(validate_iterator(prefix, "01", mock_it));

  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix, "02", mock_it));
  
  mock_tx->set(prefix, "02", _gen_val("###bar###"));
  mock->submit_transaction_sync(mock_tx);

  ASSERT_TRUE(validate_iterator(prefix, "02", mock_it));
  ASSERT_STREQ(_bl_to_str(mock_it->value()).c_str(),
	       _bl_to_str(_gen_val("02")).c_str());
}

TEST_F(SinglePrefixIteratorsTest, IteratorUpdatesMockDB)
{
  KeyValueDB::WholeSpaceIterator mock_it = mock->get_iterator();
  KeyValueDB::Transaction mock_tx = mock->get_transaction();

  mock_it->seek_to_first();
  ASSERT_TRUE(validate_iterator(prefix, "01", mock_it));

  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix, "02", mock_it));
  
  mock_tx->set(prefix, "02", _gen_val("###bar###"));
  mock->submit_transaction_sync(mock_tx);

  ASSERT_TRUE(validate_iterator(prefix, "02", mock_it));
  ASSERT_STREQ(_bl_to_str(_gen_val("###bar###")).c_str(),
	       _bl_to_str(mock_it->value()).c_str());
}

class TwoPrefixesIteratorsTest : public IteratorsTest
{
public:
  string prefix1;
  string prefix2;

  virtual void SetUp() {
    IteratorsTest::SetUp();

    prefix1 = "_01_";
    prefix2 = "_02_";

    clear(db.get());
    ASSERT_TRUE(validate_db_clear());
    clear(mock.get());
    ASSERT_TRUE(validate_db_match());

    KeyValueDB::Transaction t_db = db->get_transaction();
    t_db->set(prefix1, "01", _gen_val("01"));
    t_db->set(prefix1, "02", _gen_val("02"));
    t_db->set(prefix2, "03", _gen_val("03"));
    t_db->set(prefix2, "04", _gen_val("04"));

    KeyValueDB::Transaction t_mock = mock->get_transaction();
    t_mock->set(prefix1, "01", _gen_val("01"));
    t_mock->set(prefix1, "02", _gen_val("02"));
    t_mock->set(prefix2, "03", _gen_val("03"));
    t_mock->set(prefix2, "04", _gen_val("04"));

    db->submit_transaction_sync(t_db);
    mock->submit_transaction_sync(t_mock);

    validate_db_match();
  }

  virtual void TearDown() {
    IteratorsTest::TearDown();
  }

};

TEST_F(TwoPrefixesIteratorsTest, LowerBoundWholeSpaceIterator)
{
  // test leveldb
  KeyValueDB::WholeSpaceIterator db_it = db->get_iterator();
  db_it->lower_bound(prefix1, "");

  ASSERT_TRUE(validate_iterator(prefix1, "01", db_it));
  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix1, "02", db_it));
  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "03", db_it));
  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "04", db_it));
  db_it->next();
  ASSERT_FALSE(db_it->valid());

  db_it->lower_bound(prefix1, "01");
  ASSERT_TRUE(validate_iterator(prefix1, "01", db_it));
  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix1, "02", db_it));
  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "03", db_it));
  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "04", db_it));
  db_it->next();
  ASSERT_FALSE(db_it->valid());

  db_it->lower_bound(prefix2, "03");
  ASSERT_TRUE(validate_iterator(prefix2, "03", db_it));
  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "04", db_it));
  db_it->next();
  ASSERT_FALSE(db_it->valid());

  db_it->lower_bound(prefix2, "99");
  ASSERT_FALSE(db_it->valid());

  // test mock store
  KeyValueDB::WholeSpaceIterator mock_it = db->get_iterator();
  mock_it->lower_bound(prefix1, "");

  ASSERT_TRUE(validate_iterator(prefix1, "01", mock_it));
  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix1, "02", mock_it));
  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "03", mock_it));
  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "04", mock_it));
  mock_it->next();
  ASSERT_FALSE(mock_it->valid());

  mock_it->lower_bound(prefix1, "01");
  ASSERT_TRUE(validate_iterator(prefix1, "01", mock_it));
  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix1, "02", mock_it));
  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "03", mock_it));
  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "04", mock_it));
  mock_it->next();
  ASSERT_FALSE(mock_it->valid());

  mock_it->lower_bound(prefix2, "03");
  ASSERT_TRUE(validate_iterator(prefix2, "03", mock_it));
  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "04", mock_it));
  mock_it->next();
  ASSERT_FALSE(mock_it->valid());

  mock_it->lower_bound(prefix2, "99");
  ASSERT_FALSE(mock_it->valid());

  ASSERT_TRUE(validate_db_match());
};

TEST_F(TwoPrefixesIteratorsTest, UpperBoundWholeSpaceIterator)
{
  // test leveldb
  KeyValueDB::WholeSpaceIterator db_it = db->get_iterator();
  db_it->upper_bound(prefix1, "02");
  ASSERT_TRUE(validate_iterator(prefix2, "03", db_it));
  db_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "04", db_it));
  db_it->next();
  ASSERT_FALSE(db_it->valid());

  db_it->upper_bound(prefix2, "99");
  ASSERT_FALSE(db_it->valid());

  db_it->upper_bound(prefix2, "99");
  ASSERT_FALSE(db_it->valid());
  db_it->prev();
  ASSERT_FALSE(db_it->valid());

  db_it->upper_bound(prefix2, "04");
  ASSERT_FALSE(db_it->valid());

  db_it->upper_bound(prefix2, "03");
  ASSERT_TRUE(db_it->valid());
  ASSERT_STREQ(db_it->raw_key().first.c_str(), prefix2.c_str());
  ASSERT_STREQ(db_it->key().c_str(), "04");

  db_it->next();
  ASSERT_FALSE(db_it->valid());

  // test mock store
  KeyValueDB::WholeSpaceIterator mock_it = db->get_iterator();
  mock_it->upper_bound(prefix1, "02");
  ASSERT_TRUE(validate_iterator(prefix2, "03", mock_it));
  mock_it->next();
  ASSERT_TRUE(validate_iterator(prefix2, "04", mock_it));
  mock_it->next();
  ASSERT_FALSE(mock_it->valid());

  mock_it->upper_bound(prefix2, "99");
  ASSERT_FALSE(mock_it->valid());

  mock_it->upper_bound(prefix2, "99");
  ASSERT_FALSE(mock_it->valid());
  mock_it->prev();
  ASSERT_FALSE(mock_it->valid());

  mock_it->upper_bound(prefix2, "04");
  ASSERT_FALSE(mock_it->valid());

  mock_it->upper_bound(prefix2, "03");
  ASSERT_TRUE(mock_it->valid());
  ASSERT_STREQ(mock_it->raw_key().first.c_str(), prefix2.c_str());
  ASSERT_STREQ(mock_it->key().c_str(), "04");

  mock_it->next();
  ASSERT_FALSE(mock_it->valid());

  ASSERT_TRUE(validate_db_match());
};

TEST_F(TwoPrefixesIteratorsTest, BackwardIterationLevelDB)
{
  // test leveldb store
  KeyValueDB::WholeSpaceIterator db_it = db->get_iterator();
  db_it->seek_to_last();
  ASSERT_TRUE(db_it->valid());
  ASSERT_TRUE(validate_iterator(prefix2, "04", db_it));

  db_it->prev();
  ASSERT_TRUE(db_it->valid());
  ASSERT_TRUE(validate_iterator(prefix2, "03", db_it));

  db_it->prev();
  ASSERT_TRUE(db_it->valid());
  ASSERT_TRUE(validate_iterator(prefix1, "02", db_it));

  db_it->prev();
  ASSERT_TRUE(db_it->valid());
  ASSERT_TRUE(validate_iterator(prefix1, "01", db_it));

  db_it->prev();
  ASSERT_FALSE(db_it->valid());
}

TEST_F(TwoPrefixesIteratorsTest, BackwardIteratorMockDB)
{
  // test mock store
  KeyValueDB::WholeSpaceIterator mock_it = mock->get_iterator();
  mock_it->seek_to_last();
  ASSERT_TRUE(mock_it->valid());
  ASSERT_TRUE(validate_iterator(prefix2, "04", mock_it));

  mock_it->prev();
  ASSERT_TRUE(mock_it->valid());
  ASSERT_TRUE(validate_iterator(prefix2, "03", mock_it));

  mock_it->prev();
  ASSERT_TRUE(mock_it->valid());
  ASSERT_TRUE(validate_iterator(prefix1, "02", mock_it));

  mock_it->prev();
  ASSERT_TRUE(mock_it->valid());
  ASSERT_TRUE(validate_iterator(prefix1, "01", mock_it));

  mock_it->prev();
  ASSERT_FALSE(mock_it->valid());
}

int main(int argc, char *argv[])
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **) argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
	      << "[ceph_options] [gtest_options] <store_path>" << std::endl;
    return 1;
  }
  store_path = string(argv[1]);

  return RUN_ALL_TESTS();
}
