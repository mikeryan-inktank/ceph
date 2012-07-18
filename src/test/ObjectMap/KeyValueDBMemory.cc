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
    db_iter = db->db.end();
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
    db_iter = db->db.end();
    if ((db->db.size() == 0 ) || (db->db.count(prefix) == 0)) {
      ready = false;
      return 0;
    }
    db_iter = db->db.find(prefix);
    curr_iter = (*db_iter).second.begin();
    assert(curr_iter == db->db[prefix].begin());
    ready = true;
    return 0;
  }

  int seek_to_last() {
    db_iter = db->db.end();
    if (db->db.size() == 0) {
      ready = false;
      return 0;
    }
    db_iter = --db->db.end();
    assert(db_iter != db->db.end());
    assert((*db_iter).second.size() > 0);
    curr_iter = --(*db_iter).second.end();
    assert(curr_iter != (*db_iter).second.end());
    ready = true;
    return 0;
  }

  int seek_to_last(const string &prefix) {
    db_iter = db->db.end();
    if ((db->db.size() == 0) || (db->db.count(prefix) == 0)) {
      ready = false;
      return 0;
    }
    db_iter = db->db.find(prefix);
    assert((*db_iter).second.size() > 0);
    curr_iter = --(*db_iter).second.end();
    ready = true;
    return 0;
  }

  int lower_bound(const string &prefix, const string &to) {
    db_iter = db->db.end();
    if ((db->db.size() == 0) || (db->db.count(prefix) == 0)) {
      ready = false;
      return 0;
    }
    db_iter = db->db.find(prefix);
    assert(db_iter != db->db.end());
    curr_iter = (*db_iter).second.lower_bound(to);
    ready = true;
    return 0;
  }

  int upper_bound(const string &prefix, const string &after) {
    db_iter = db->db.end();
    if ((db->db.size() == 0) || (db->db.count(prefix) == 0)) {
      ready = false;
      return 0;
    }
    db_iter = db->db.find(prefix);
    assert(db_iter != db->db.end());
    curr_iter = (*db_iter).second.upper_bound(after);
    ready = true;
    return 0;
  }

  bool valid() {
    return ready && (db_iter != db->db.end())
      && (curr_iter != (*db_iter).second.end());
  }

  bool begin() {
    return ready && (db_iter == db->db.begin())
      && (curr_iter == (*db_iter).second.begin());
  }

  int prev() {
    if (begin() || !ready)
      return 0;

    if (_prev_adjust_iterators()) {
      return 0;
    }
    --curr_iter;
    _prev_adjust_iterators();
    return 0;
  }

  int next() {
    if (!valid())
      return 0;

    if (_next_adjust_iterators()) {
      return 0;
    }
    ++curr_iter;
    _next_adjust_iterators();
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

private:
  /**
   * Adjust iterators if we hit a boundary during a 'next()' call
   *
   * When we are advancing the iterator, we may hit a map boundary. In
   * this event, we must adjust our iterators to match the expected
   * result.
   *
   * We will adjust the iterators if the curr_iter has reached the end of
   * the current db_iter position; we must then move db_iter forward, and
   * assign curr_iter to the first position of db_iter iif db_iter didn't
   * reach the end of the db map.
   *
   * @returns true if we adjusted the iterators; false otherwise.
   */
  bool _next_adjust_iterators() {
    if (curr_iter == (*db_iter).second.end()) {
      ++db_iter;
      if (db_iter == db->db.end())
	return true;
      curr_iter = (*db_iter).second.begin();
      assert(curr_iter != (*db_iter).second.end());
      return true;
    }
    return false;
  }
  /**
   * Adjust iterators if we hit a boundary during a 'prev()' call
   *
   * When we are call the 'prev()' function, the iterator may hit a map
   * boundary. In this event, we must adjust our iterators to match the
   * expected result.
   *
   * If the 'db_iter' is on the beginning of the db map, and the 'curr_iter'
   * is on the beginning of the map on the 'db_iter' position, then we
   * can't do anything, so we will just return.
   *
   * On the other hand, if 'db_iter' is not on the first position, but
   * 'curr_iter' is on the first position of the map on 'db_iter's
   * position, then we will have the go back to the previous 'db_iter' map
   * and set the 'curr_iter' on the first valid position before the end
   * of that map.
   *
   * Otherwise, we have nothing to adjust.
   *
   * @returns true if we adjusted the iterators; false otherwise.
   */
  bool _prev_adjust_iterators() {
    if (db_iter == db->db.begin()) {
      if (curr_iter == (*db_iter).second.begin())
	return true;
    } else if (curr_iter == (*db_iter).second.begin()) {
      --db_iter;
      if ((*db_iter).second.size() > 0)
	curr_iter = --(*db_iter).second.end();
      return true;
    }
    return false;
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

class WholeSpaceSnapshotMemIterator : public WholeSpaceMemIterator {
public:
  WholeSpaceSnapshotMemIterator(KeyValueDBMemory *db) :
    WholeSpaceMemIterator(db) { }
  ~WholeSpaceSnapshotMemIterator() {
    delete db;
  }
};

KeyValueDB::WholeSpaceIterator KeyValueDBMemory::_get_snapshot_iterator() {
  KeyValueDBMemory *snap_db = new KeyValueDBMemory(this);
  return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
    new WholeSpaceSnapshotMemIterator(snap_db)
  );
}
