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

  string prefix;

  virtual void SetUp() {
    assert(!store_path.empty());

    prefix = "_TEST_";

    LevelDBStore *db_ptr = new LevelDBStore(store_path);
    assert(!db_ptr->init(std::cerr));
    db.reset(db_ptr);

    mock.reset(new KeyValueDBMemory());
  }

  virtual void TearDown() { }

  void validate_db_clear() {
    KeyValueDB::WholeSpaceIterator it = db->get_iterator();
    it->seek_to_first();
    while (it->valid()) {
      pair<string,string> k = it->raw_key();
      ASSERT_EQ((size_t) 0, mock->db.count(k.first));
      it->next();
    }
  }

  void validate_db_match() {
    KeyValueDB::WholeSpaceIterator it = db->get_iterator();
    it->seek_to_first();
    while (it->valid()) {
      pair<string, string> k = it->raw_key();
      ASSERT_GT(mock->db.count(k.first), (size_t) 0);
      ASSERT_GT(mock->db[k.first].count(k.second), (size_t) 0);

      bufferlist it_bl = it->value();
      bufferlist mock_bl = mock->db[k.first][k.second];

      string it_val = _bl_to_str(it_bl);
      string mock_val = _bl_to_str(mock_bl);

      ASSERT_STREQ(it_val.c_str(), mock_val.c_str());
      it->next();
    }
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

  void clear() {
    clear(db.get());
    validate_db_clear();
    clear(mock.get());
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
    it->seek_to_first();
    int i = 0;
    while (it->valid()) {
      std::cerr << "#" << (++i) << " > key: " << it->key()
		<< " value: " << it->value() << std::endl;
      it->next();
    }
  }

  void _test_rmkeys_setup() {
   clear();

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

    validate_db_match();
  }

  void _test_rmkeys_iterator(KeyValueDB *db,
			     KeyValueDB::WholeSpaceIterator it) {

    it->seek_to_first();
    ASSERT_TRUE(it->valid());

    KeyValueDB::Transaction t = db->get_transaction();
    t->rmkey(prefix, "01");
    t->rmkey(prefix, "02");
    db->submit_transaction_sync(t);

    ASSERT_TRUE(it->valid());
    ASSERT_STREQ(it->key().c_str(), "01");
    ASSERT_STREQ(_bl_to_str(it->value()).c_str(),
		 _bl_to_str(_gen_val("01")).c_str());

    it->next();
    ASSERT_TRUE(it->valid());
    it->next();
    ASSERT_TRUE(it->valid());

    ASSERT_STREQ(it->key().c_str(), "03");
    ASSERT_STREQ(_bl_to_str(it->value()).c_str(),
		 _bl_to_str(_gen_val("03")).c_str());

    std::cerr << __func__ << " okay" << std::endl;
  }

  void test_rmkeys() {
    _test_rmkeys_setup();

    /* The LevelDBStore's iterator appears to maintain the same state it had
     * once the iteratio began, even if we remove keys from the store. Thus we
     * don't have any problems with moving the iterator's position.
     *
     * However, the KeyValueDBMemory iterator is unable to cope with such
     * operations, and will segfault once we try to use the iterator's handle
     * after removing the item the handle was supposed to point to.
     */
    std::cerr << __func__ << " test LevelDBStore iterator" << std::endl;
    _test_rmkeys_iterator(db.get(), db->get_iterator());
    /*
     * This will segfault
     */
#if 0
    std::cerr << __func__ << " test KeyValueDBMemory iterator" << std::endl;
    _test_rmkeys_iterator(mock.get(), mock->get_iterator());

    validate_db_match();
#endif

    _test_rmkeys_setup();

    std::cerr << __func__ << " test LevelDBStore read-only iterator"
	      << std::endl;
    _test_rmkeys_iterator(db.get(), db->get_snapshot_iterator());
    std::cerr << __func__ << " test KeyValueDBMemory read-only iterator"
	      << std::endl;
    _test_rmkeys_iterator(db.get(), mock->get_snapshot_iterator());

    validate_db_match();
  }

};

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

TEST_F(IteratorsTest, RemoveTest) {
  test_rmkeys();
}
