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
#include <iostream>
#include <string>
#include <sstream>
#include <boost/scoped_ptr.hpp>

#include "os/LevelDBStore.h"
#include "test/ObjectMap/KeyValueDBMemory.h"

using namespace std;

static inline ostream& operator<<(ostream& out, pair<string,string> key) {
  out << "key( " << key.first << "," << key.second << " )";
  return out;
}

static inline void print_bl(ostream& out, bufferlist value) {
  out << "value( " << string(value.c_str(), value.length()) << " )";
  //  ostringstream os;
  //  value.hexdump(os);
  //  out << "value(\n" << os.str() << ")";
}

class Validator {

  KeyValueDB *a;
  KeyValueDB *b;

  public:

  Validator(KeyValueDB *a, KeyValueDB *b) : a(a), b(b) { }

  bool validate_keys(pair<string,string> a, pair<string,string> b) {
    return ((a.first == b.first) && (a.second != b.second));
  }

  bool validate_values(bufferlist a, bufferlist b) {
    return (a.contents_equal(b));
  }

  string print_key(pair<string,string> key) {
    ostringstream os;
    os << "key( " << key.first << "," << key.second << ")";
    return os.str();
  }

  string print_values(bufferlist value) {
    ostringstream os;
    os << "value(\n";
    value.hexdump(os);
    os << ")";
    return os.str();
  }

  void print_non_existents(map<pair<string,string>,bufferlist>& m) {
    map<pair<string,string>,bufferlist>::iterator it;

    for (it = m.begin(); it != m.end(); ++it) {
      std::cout << "  " << (*it).first << " ";
      print_bl(std::cout, (*it).second);
      std::cout << std::endl;
    }
  }

  void print_mismatches(KeyValueDB *db,
			map<pair<string,string>,bufferlist>& m) {
    map<pair<string,string>,bufferlist>::iterator it;

    for (it = m.begin(); it != m.end(); ++it) {
      std::set<string> k;
      std::map<string,bufferlist> v;

      pair<string,string> m_raw_key = (*it).first;
      k.insert(m_raw_key.second);
      db->get(m_raw_key.first, k, &v);

      std::cout << "  "  << m_raw_key << std::endl;
      std::cout << "    expected ";
      print_bl(std::cout, v[m_raw_key.second]);
      std::cout << std::endl;
      std::cout << "    found ";
      print_bl(std::cout, (*it).second);
      std::cout << std::endl;
    }
  }

  bool _validate_relaxed(KeyValueDB *a, KeyValueDB *b,
			 map<pair<string,string>,bufferlist>& ne,
			 map<pair<string,string>,bufferlist>& mismatch) {

    KeyValueDB::WholeSpaceIterator a_it = a->get_iterator();
    a_it->seek_to_first();

    for (; a_it->valid(); a_it->next()) {

      pair<string,string> raw_key = a_it->raw_key();
      bufferlist expected_value = a_it->value();

      std::set<string> k;
      std::map<string, bufferlist> v;

      k.insert(raw_key.second);
      b->get(raw_key.first, k, &v);

      if (v.size() == 0) {
	ne.insert(make_pair(
	      make_pair(raw_key.first,raw_key.second),
	      expected_value)
	    );
	continue;
      }

      if (!validate_values(expected_value, v[raw_key.second])) {
	mismatch.insert(make_pair(
	      make_pair(raw_key.first, raw_key.second),
	      v[raw_key.second])
	    );
	continue;
      }

    }

    return ( (ne.size() == 0) && (mismatch.size() == 0) );

  }

  bool validate_relaxed(bool die_on_mismatch = false) {

    map<pair<string,string>, bufferlist> a_ne_map;
    map<pair<string,string>, bufferlist> a_mismatch_map;

    map<pair<string,string>, bufferlist> b_ne_map;
    map<pair<string,string>, bufferlist> b_mismatch_map;

    bool match = true;
    if (!_validate_relaxed(a, b, a_ne_map, a_mismatch_map))
      match = false;

    if (!_validate_relaxed(b, a, b_ne_map, b_mismatch_map))
      match = false;

    if (a_ne_map.size() > 0) {
      std::cout << __func__ << " exists on A and not on B:" << std::endl;
      print_non_existents(a_ne_map);
    }

    if (a_mismatch_map.size() > 0) {
      std::cout << __func__ << " values differ on A and B:" << std::endl;
      print_mismatches(a, a_mismatch_map);
    }

    if (b_ne_map.size() > 0) {
      std::cout << __func__ << " exists on B and not on A:" << std::endl;
      print_non_existents(b_ne_map);
    }

    if (b_mismatch_map.size() > 0) {
      std::cout << __func__ << " values differ on B and A:" << std::endl;
      print_mismatches(b, b_mismatch_map);
    }

    return match;
  }

  bool validate_strict(bool die_on_mismatch = false) {
    KeyValueDB::WholeSpaceIterator a_it = a->get_iterator();
    KeyValueDB::WholeSpaceIterator b_it = b->get_iterator();

    a_it->seek_to_first();
    b_it->seek_to_first();

    bool invalid = false;
    while (true) {
      if (!a_it->valid()) {
	std::cout << __func__ << " A reached its end" << std::endl;
	invalid = true;
      }
      if (!b_it->valid()) {
	std::cout << __func__ << " B reached its end" << std::endl;
	invalid = (invalid ? false : true);
	break;
      }

      pair<string,string> a_key = a_it->raw_key();
      pair<string,string> b_key = b_it->raw_key();

      bool match = true;
      match = validate_keys(a_key, b_key);
      if (!match) {
	std::cout << __func__
	  << " A( " << a_key << " )"
	  << " != "
	  << " B( " << b_key << " )"
	  << std::endl;
      }
      match = validate_values(a_it->value(), b_it->value());
      if (!match) {
	std::cout << __func__
	  << " A( " << a_key << " ";
	print_bl(std::cout, a_it->value());
	std::cout << " )"
	  << " != "
	  << " B( " << b_key << " ";
	print_bl(std::cout, b_it->value());
	std::cout << " )"
	  << std::endl;
      }

      if (!match) {
	invalid = true;
	if (die_on_mismatch)
	  break;
      }
      a_it->next();
      b_it->next();
    }

    if (!invalid)
      std::cout << __func__ << " A and B match" << std::endl;
    else
      std::cout << __func__ << " A and B do not match" << std::endl;

    return !invalid;
  }
};

bufferlist _get_bl(string s)
{
  bufferlist bl;
  bl.append(s);
  return bl;
}

void run_test()
{
  boost::scoped_ptr<KeyValueDB> a;
  boost::scoped_ptr<KeyValueDB> b;

  a.reset(new KeyValueDBMemory());
  LevelDBStore *b_store = new LevelDBStore("test_store_B");
  assert(!b_store->init(std::cerr));
  b.reset(b_store);

  KeyValueDB::Transaction t_a = a->get_transaction();
  t_a->set("foo", "bar", _get_bl("###foobar###"));
  t_a->set("boo", "yah", _get_bl("###booyah###"));
  t_a->set("foo", "gaz", _get_bl("###foogaz###"));
  t_a->set("boo", "hoo", bufferlist());
  a->submit_transaction_sync(t_a);

  KeyValueDB::Transaction t_b = b->get_transaction();
  t_b->set("foo", "tah", _get_bl("---footah---"));
  t_b->set("foo", "bar", _get_bl("###foobar###"));
  t_b->set("foo", "gaz", _get_bl("###foogaz###"));
  t_b->set("boo", "hoo", _get_bl("---boohoo--"));
  b->submit_transaction_sync(t_b);

  Validator validator(a.get(), b.get());

  std::cout << "--- Strict Validation:" << std::endl;
  bool strict_validation = validator.validate_strict();
  std::cout << "--- Returns: " << strict_validation << std::endl;

  std::cout << "--- Relaxed Validation:" << std::endl;
  bool relaxed_validation = validator.validate_relaxed();
  std::cout << "--- Returns: " << relaxed_validation << std::endl;
}

int main(int argc, char *argv[])
{
  run_test();

  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <store path> <prefix>" << std::endl;
    return 1;
  }

  string path(argv[1]);
  string prefix(argv[2]);
  std::cout << "path: " << path << " ; prefix: " << prefix << std::endl;

  LevelDBStore ldb(path);
  assert(!ldb.init(std::cerr));

  KeyValueDB::WholeSpaceIterator it = ldb.get_iterator();
  it->seek_to_first();
  while (it->valid()) {
    pair<string,string> raw_key = it->raw_key();
    std::cout << "prefix = " << raw_key.first
      << " key = " << raw_key.second << std::endl;
    ostringstream os;
    it->value().hexdump(os);
    string hex = os.str();
    std::cout << hex << std::endl;
    std::cout << "--------------------------------------------" << std::endl;

    it->next();
  }

  return 0;
}
