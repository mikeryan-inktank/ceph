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

using namespace std;

class IteratorsTest {

  boost::scoped_ptr<KeyValueDB> db;
  boost::scoped_ptr<KeyValueDBMemory> mock;

  const string PREFIX;

public:
  IteratorsTest(string path) : PREFIX("_TEST_") {
    LevelDBStore *db_ptr = new LevelDBStore(path);
    assert(!db_ptr->init(std::cerr));
    db.reset(db_ptr);

    mock.reset(new KeyValueDBMemory());
  }

  void validate_db_clear() {
    KeyValueDB::WholeSpaceIterator it = db->get_iterator();
    it->seek_to_first();
    while (it->valid()) {
      pair<string,string> k = it->raw_key();
      assert(mock->db.count(k.first) == 0);
      it->next();
    }
  }

  void validate_db_match() {
    KeyValueDB::WholeSpaceIterator it = db->get_iterator();
    it->seek_to_first();
    while (it->valid()) {
      pair<string, string> k = it->raw_key();
      assert(mock->db.count(k.first) > 0);
      assert(mock->db[k.first].count(k.second) > 0);

      bufferlist it_bl = it->value();
      bufferlist mock_bl = mock->db[k.first][k.second];

      string it_val = _bl_to_str(it_bl);
      string mock_val = _bl_to_str(mock_bl);

      assert(it_val == mock_val);
      it->next();
    }
  }

  void clear(KeyValueDB *db) {
    KeyValueDB::WholeSpaceIterator it = db->get_readonly_iterator();
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
      std::cout << "#" << (++i) << " > key: " << it->key()
		<< " value: " << it->value() << std::endl;
      it->next();
    }
  }

  void _test_rmkeys_setup() {
   clear();

    KeyValueDB::Transaction t_db = db->get_transaction();
    t_db->set(PREFIX, "01", _gen_val("01"));
    t_db->set(PREFIX, "02", _gen_val("02"));
    t_db->set(PREFIX, "03", _gen_val("03"));

    KeyValueDB::Transaction t_mock = mock->get_transaction();
    t_mock->set(PREFIX, "01", _gen_val("01"));
    t_mock->set(PREFIX, "02", _gen_val("02"));
    t_mock->set(PREFIX, "03", _gen_val("03"));

    db->submit_transaction_sync(t_db);
    mock->submit_transaction_sync(t_mock);

    validate_db_match();
  }

  void _test_rmkeys_iterator(KeyValueDB *db,
			     KeyValueDB::WholeSpaceIterator it) {

    it->seek_to_first();
    assert(it->valid());

    KeyValueDB::Transaction t = db->get_transaction();
    t->rmkey(PREFIX, "01");
    t->rmkey(PREFIX, "02");
    db->submit_transaction_sync(t);

    assert(it->valid());
    assert(it->key() == "01");
    assert(_bl_to_str(it->value()) == _bl_to_str(_gen_val("01")));

    it->next();
    assert(it->valid());
    it->next();
    assert(it->valid());

    assert(it->key() == "03");
    assert(_bl_to_str(it->value()) == _bl_to_str(_gen_val("03")));

    std::cout << __func__ << " okay" << std::endl;
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
    std::cout << __func__ << " test LevelDBStore iterator" << std::endl;
    _test_rmkeys_iterator(db.get(), db->get_iterator());
    /*
     * This will segfault
     */
#if 0
    std::cout << __func__ << " test KeyValueDBMemory iterator" << std::endl;
    _test_rmkeys_iterator(mock.get(), mock->get_iterator());

    validate_db_match();
#endif

    _test_rmkeys_setup();

    std::cout << __func__ << " test LevelDBStore read-only iterator"
	      << std::endl;
    _test_rmkeys_iterator(db.get(), db->get_readonly_iterator());
    std::cout << __func__ << " test KeyValueDBMemory read-only iterator"
	      << std::endl;
    _test_rmkeys_iterator(db.get(), mock->get_readonly_iterator());

    validate_db_match();
  }

};

int main(int argc, char *argv[]) {

  boost::scoped_ptr<IteratorsTest> test;

  if (argc < 2) {
    std::cout << "Usage: " << argv[0] << " <store_path>" << std::endl;
    return 1;
  }
  string path(argv[1]);
  test.reset(new IteratorsTest(path));

  test->test_rmkeys();

  return 0;
}

