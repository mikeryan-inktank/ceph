// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "include/encoding.h"
#include "KeyValueDBMemory.h"
#include <map>
#include <set>
#include <tr1/memory>
#include <iostream>

using namespace std;

class WholeSpaceMemIterator : public KeyValueDB::WholeSpaceIteratorImpl {
protected:
  KeyValueDBMemory *db;
  bool ready;

  map<string, map<string, bufferlist> >::iterator db_iter;
  map<string, bufferlist>::iterator curr_iter;

public:
  WholeSpaceMemIterator(KeyValueDBMemory *db) : db(db), ready(false) { }
  virtual ~WholeSpaceMemIterator() { }

  int seek_to_first() {
    if (db->db.size() == 0) {
      ready = false;
      return 0;
    }
    db_iter = db->db.begin();
    curr_iter = (*db_iter).second.begin();
    ready = true;
    return 0;
  }

  int seek_to_first(const string &prefix) {
    if ((db->db.size() == 0) || (db->db.count(prefix) == 0)) {
      ready = false;
      return 0;
    }
    db_iter = db->db.find(prefix);
    curr_iter = (*db_iter).second.begin();
    assert(curr_iter == db->db[prefix].begin());
    return 0;
  }

  int seek_to_last() {
    if (db->db.size() == 0) {
      ready = false;
      return 0;
    }
    db_iter = --db->db.end();
    curr_iter = --(*db_iter).second.end();
    ready = true;
    return 0;
  }

  int seek_to_last(const string &prefix) {
    if ((db->db.size() == 0) || (db->db.count(prefix) == 0)) {
      ready = false;
      return 0;
    }
    db_iter = db->db.find(prefix);
    curr_iter = --(*db_iter).second.end();
    return 0;
  }

  int lower_bound(const string &prefix, const string &to) {
    if ((db->db.size() == 0) || (db->db.count(prefix) == 0)) {
      ready = false;
      return 0;
    }
    db_iter = db->db.find(prefix);
    curr_iter = (*db_iter).second.lower_bound(to);
    ready = true;
    return 0;
  }

  int upper_bound(const string &prefix, const string &after) {
    if ((db->db.size() == 0) || (db->db.count(prefix) == 0)) {
      ready = false;
      return 0;
    }
    db_iter = db->db.find(prefix);
    curr_iter = (*db_iter).second.upper_bound(after);
    ready = true;
    return 0;
  }

  bool valid() {
    return ready && (db_iter != db->db.end());
  }

  bool begin() {
    return ready && (db_iter == db->db.begin())
      && (curr_iter == (*db_iter).second.begin());
  }

  int prev() {
    if (!valid())
      return 0;

    if (db_iter == db->db.begin()) {
      if (curr_iter == (*db_iter).second.begin())
	return 0;
    } else if (curr_iter == (*db_iter).second.begin()) {
      --db_iter;
      curr_iter = --(*db_iter).second.end();
      return 0;
    }

    --curr_iter;
    return 0;
  }

  int next() {
    if (!valid() || (db_iter == db->db.end()))
      return 0;
    if (curr_iter == (*db_iter).second.end()) {
      ++db_iter;
      if (db_iter == db->db.end())
	return 0;
      curr_iter = (*db_iter).second.begin();
      return 0;
    }
    ++curr_iter;
    return 0;
  }

  string key() {
    if (valid())
      return (*curr_iter).first;
    else
      return "";
  }

  pair<string,string> raw_key() {
    if (valid())
      return make_pair((*db_iter).first, (*curr_iter).first);
    else
      return make_pair("", "");
  }

  bufferlist value() {
    if (valid())
      return (*curr_iter).second;
    else
      return bufferlist();
  }

  int status() {
    return 0;
  }

};

int KeyValueDBMemory::get(const string &prefix,
			  const std::set<string> &key,
			  map<string, bufferlist> *out) {
  if (!db.count(prefix))
    return 0;

  for (std::set<string>::const_iterator i = key.begin();
       i != key.end();
       ++i) {
    if (db[prefix].count(*i))
      (*out)[*i] = db[prefix][*i];
  }
  return 0;
}

int KeyValueDBMemory::get_keys(const string &prefix,
			       const std::set<string> &key,
			       std::set<string> *out) {
  if (!db.count(prefix))
    return 0;

  for (std::set<string>::const_iterator i = key.begin();
       i != key.end();
       ++i) {
    if (db[prefix].count(*i))
      out->insert(*i);
  }
  return 0;
}

int KeyValueDBMemory::set(const string &prefix,
			  const string &key,
			  const bufferlist &bl) {
  db[prefix][key] = bl;
  return 0;
}

int KeyValueDBMemory::set(const string &prefix,
			  const map<string, bufferlist> &to_set) {
  for (map<string, bufferlist>::const_iterator i = to_set.begin();
       i != to_set.end();
       ++i) {
    set(prefix, i->first, i->second);
  }
  return 0;
}

int KeyValueDBMemory::rmkey(const string &prefix,
			    const string &key) {
  db[prefix].erase(key);
  return 0;
}

int KeyValueDBMemory::rmkeys(const string &prefix,
			     const std::set<string> &keys) {
  if (!db.count(prefix))
    return 0;
  for (std::set<string>::const_iterator i = keys.begin();
       i != keys.end();
       ++i) {
    rmkey(prefix, *i);
  }
  return 0;
}

int KeyValueDBMemory::rmkeys_by_prefix(const string &prefix) {
  db.erase(prefix);
  return 0;
}

KeyValueDB::WholeSpaceIterator KeyValueDBMemory::_get_iterator() {
  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new WholeSpaceMemIterator(this)
  );
}

class WholeSpaceReadOnlyMemIterator : public WholeSpaceMemIterator {
public:
  WholeSpaceReadOnlyMemIterator(KeyValueDBMemory *db) :
    WholeSpaceMemIterator(db) { }
  ~WholeSpaceReadOnlyMemIterator() {
    delete db;
  }
};

KeyValueDB::WholeSpaceIterator KeyValueDBMemory::_get_readonly_iterator() {
  KeyValueDBMemory *ro_db = new KeyValueDBMemory(this);
  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new WholeSpaceReadOnlyMemIterator(ro_db)
  );
}
