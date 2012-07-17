// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KEY_VALUE_DB_H
#define KEY_VALUE_DB_H

#include "include/buffer.h"
#include <set>
#include <map>
#include <string>
#include <tr1/memory>
#include <boost/scoped_ptr.hpp>
#include "ObjectMap.h"

using std::string;

#if 1
#define OUT std::cout << "kv::" << __func__
#define COUT std::cout
#define ENDL std::endl
#else
#define OUT do { ostringstream _os; _os
#define COUT do { ostringstream _os; _os
#define ENDL "\n"; } while(0)
#endif


/**
 * Defines virtual interface to be implemented by key value store
 *
 * Kyoto Cabinet or LevelDB should implement this
 */
class KeyValueDB {
public:
  class TransactionImpl {
  public:
    /// Set Keys
    void set(
      const string &prefix,                 ///< [in] Prefix for keys
      const std::map<string, bufferlist> &to_set ///< [in] keys/values to set
    ) {
      std::map<string, bufferlist>::const_iterator it;
      for (it = to_set.begin(); it != to_set.end(); ++it)
	set(prefix, it->first, it->second);
    }

    /// Set Key
    virtual void set(
      const string &prefix,   ///< [in] Prefix for the key
      const string &k,	      ///< [in] Key to set
      const bufferlist &bl    ///< [in] Value to set
      ) = 0;


    /// Removes Keys
    void rmkeys(
      const string &prefix,   ///< [in] Prefix to search for
      const std::set<string> &keys ///< [in] Keys to remove
    ) {
      std::set<string>::const_iterator it;
      for (it = keys.begin(); it != keys.end(); ++it)
	rmkey(prefix, *it);
    }

    /// Remove Key
    virtual void rmkey(
      const string &prefix,   ///< [in] Prefix to search for
      const string &k	      ///< [in] Key to remove
      ) = 0;

    /// Removes keys beginning with prefix
    virtual void rmkeys_by_prefix(
      const string &prefix ///< [in] Prefix by which to remove keys
      ) = 0;

    virtual ~TransactionImpl() {};
  };
  typedef std::tr1::shared_ptr< TransactionImpl > Transaction;

  virtual Transaction get_transaction() = 0;
  virtual int submit_transaction(Transaction) = 0;
  virtual int submit_transaction_sync(Transaction t) {
    return submit_transaction(t);
  }

  /// Retrieve Keys
  virtual int get(
    const string &prefix,        ///< [in] Prefix for key
    const std::set<string> &key,      ///< [in] Key to retrieve
    std::map<string, bufferlist> *out ///< [out] Key value retrieved
    ) = 0;

  class WholeSpaceIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int seek_to_first(const string &prefix) = 0;
    virtual int seek_to_last() = 0;
    virtual int seek_to_last(const string &prefix) = 0;
    virtual int upper_bound(const string &prefix, const string &after) = 0;
    virtual int lower_bound(const string &prefix, const string &to) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual int prev() = 0;
    virtual string key() = 0;
    virtual pair<string,string> raw_key() = 0;
    virtual bufferlist value() = 0;
    virtual int status() = 0;
    virtual ~WholeSpaceIteratorImpl() { }
  };
  typedef std::tr1::shared_ptr< WholeSpaceIteratorImpl > WholeSpaceIterator;

  class IteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    const string prefix;
    WholeSpaceIterator generic_iter;
  public:
    IteratorImpl(const string &prefix, WholeSpaceIterator iter) :
      prefix(prefix), generic_iter(iter) { }
    virtual ~IteratorImpl() { }

    int seek_to_first() {
      OUT << " prefix " << prefix << ENDL;
      int r = generic_iter->seek_to_first(prefix);
      OUT << " ret " << r << ENDL;
      return r;
    }
    int seek_to_last() {
      OUT << " prefix " << prefix << ENDL;
      int r = generic_iter->seek_to_last(prefix);
      OUT << " ret " << r << ENDL;
      return r;
    }
    int upper_bound(const string &after) {
      OUT << " prefix " << prefix << " after " << after << ENDL;
      int r = generic_iter->upper_bound(prefix, after);
      OUT << " ret " << r << ENDL;
      return r;
    }
    int lower_bound(const string &to) {
      OUT << " prefix " << prefix << " to " << to << ENDL;
      int r = generic_iter->lower_bound(prefix, to);
      OUT << " ret " << r << ENDL;
      return r;
    }
    bool valid() {
      OUT << ENDL;
      bool r;
      if (!generic_iter->valid()) {
	OUT << " whole-space not valid" << ENDL;
	r = false;
      } else {
	pair<string,string> raw_key = generic_iter->raw_key();
	r = (raw_key.first == prefix);
	OUT << " raw_key(" << raw_key.first << "," << raw_key.second
	    << ") prefix " << prefix << " ret " << r << ENDL;
      }
      return r;
    }
    int next() {
      OUT << ENDL;
      int r;
      if (valid()) {
	r = generic_iter->next();
	OUT << " ret " << r << " (valid)" << ENDL;
      } else {
	r = status();
	OUT << " ret " << r << " (invalid - from status)" << ENDL;
      }
      return r;
    }
    int prev() {
      OUT << ENDL;
      int r;
      if (valid()) {
	r = generic_iter->prev();
	OUT << " ret " << r << " (valid)" << ENDL;
      } else {
	r = status();
	OUT << " ret " << r << " (invalid - from status)" << ENDL;
      }
      return r;
    }
    string key() {
      string r = generic_iter->key();
      OUT << " ret " << r << ENDL;
      return r;
    }
    bufferlist value() {
      bufferlist bl = generic_iter->value();
      ostringstream os;
      bl.hexdump(os);
      string hex = os.str();
      OUT << " bytes " << bl.length() << ENDL;
      COUT << hex << ENDL;
      return bl;
    }
    int status() {
      int r = generic_iter->status();
      OUT << " ret " << r << ENDL;
      return r;
    }
  };

  typedef std::tr1::shared_ptr< IteratorImpl > Iterator;

  WholeSpaceIterator get_iterator() {
    return _get_iterator();
  }

  Iterator get_iterator(const string &prefix) {
    OUT << __func__ << " prefix " << prefix << ENDL;
    return std::tr1::shared_ptr<IteratorImpl>(
      new IteratorImpl(prefix, get_iterator())
    );
  }

  WholeSpaceIterator get_readonly_iterator() {
    return _get_readonly_iterator();
  }

  Iterator get_readonly_iterator(const string &prefix) {
    return std::tr1::shared_ptr<IteratorImpl>(
      new IteratorImpl(prefix, get_readonly_iterator())
    );
  }

  virtual ~KeyValueDB() {}

protected:
  virtual WholeSpaceIterator _get_iterator() = 0;
  virtual WholeSpaceIterator _get_readonly_iterator() = 0;
};

#endif
