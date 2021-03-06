// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#ifndef CEPH_FILESTORE_H
#define CEPH_FILESTORE_H

#include "include/types.h"

#include <map>
#include <deque>
#include <boost/scoped_ptr.hpp>
#include <fstream>
using namespace std;

#include <ext/hash_map>
using namespace __gnu_cxx;

#include "include/assert.h"

#include "ObjectStore.h"
#include "JournalingObjectStore.h"

#include "common/Timer.h"
#include "common/WorkQueue.h"

#include "common/Mutex.h"
#include "HashIndex.h"
#include "IndexManager.h"
#include "ObjectMap.h"
#include "SequencerPosition.h"

#include "include/uuid.h"


// from include/linux/falloc.h:
#ifndef FALLOC_FL_PUNCH_HOLE
# define FALLOC_FL_PUNCH_HOLE 0x2
#endif

class FileStore : public JournalingObjectStore,
                  public md_config_obs_t
{
public:
  static const uint32_t on_disk_version = 3;
private:
  string internal_name;         ///< internal name, used to name the perfcounter instance
  string basedir, journalpath;
  std::string current_fn;
  std::string current_op_seq_fn;
  std::string omap_dir;
  uuid_d fsid;
  
  bool btrfs;                   ///< fs is btrfs
  bool btrfs_stable_commits;    ///< we are using btrfs snapshots for a stable journal refernce
  uint64_t blk_size;            ///< fs block size
  bool btrfs_trans_start_end;   ///< btrfs trans start/end ioctls are supported
  bool btrfs_clone_range;       ///< btrfs clone range ioctl is supported
  bool btrfs_snap_create;       ///< btrfs snap create ioctl is supported
  bool btrfs_snap_destroy;      ///< btrfs snap destroy ioctl is supported
  bool btrfs_snap_create_v2;    ///< btrfs snap create v2 ioctl (async!) is supported
  bool btrfs_wait_sync;         ///< btrfs wait sync ioctl is supported
  bool ioctl_fiemap;            ///< fiemap ioctl is supported
  int fsid_fd, op_fd;

  int basedir_fd, current_fd;
  deque<uint64_t> snaps;

  // Indexed Collections
  IndexManager index_manager;
  int get_index(coll_t c, Index *index);
  int init_index(coll_t c);

  // ObjectMap
  boost::scoped_ptr<ObjectMap> object_map;
  
  Finisher ondisk_finisher;

  // helper fns
  int get_cdir(coll_t cid, char *s, int len);
  
  /// read a uuid from fd
  int read_fsid(int fd, uuid_d *uuid);

  /// lock fsid_fd
  int lock_fsid();

  // sync thread
  Mutex lock;
  bool force_sync;
  Cond sync_cond;
  uint64_t sync_epoch;

  Mutex sync_entry_timeo_lock;
  SafeTimer timer;

  list<Context*> sync_waiters;
  bool stop;
  void sync_entry();
  struct SyncThread : public Thread {
    FileStore *fs;
    SyncThread(FileStore *f) : fs(f) {}
    void *entry() {
      fs->sync_entry();
      return 0;
    }
  } sync_thread;

  void trigger_commit(uint64_t);

  void sync_fs(); // actuall sync underlying fs

  // -- op workqueue --
  struct Op {
    utime_t start;
    uint64_t op;
    list<Transaction*> tls;
    Context *onreadable, *onreadable_sync;
    uint64_t ops, bytes;
    TrackedOpRef osd_op;
  };
  class OpSequencer : public Sequencer_impl {
    Mutex qlock; // to protect q, for benefit of flush (peek/dequeue also protected by lock)
    list<Op*> q;
    list<uint64_t> jq;
    Cond cond;
  public:
    Sequencer *parent;
    Mutex apply_lock;  // for apply mutual exclusion
    
    void queue_journal(uint64_t s) {
      Mutex::Locker l(qlock);
      jq.push_back(s);
    }
    void dequeue_journal() {
      Mutex::Locker l(qlock);
      jq.pop_front();
      cond.Signal();
    }
    void queue(Op *o) {
      Mutex::Locker l(qlock);
      q.push_back(o);
    }
    Op *peek_queue() {
      assert(apply_lock.is_locked());
      return q.front();
    }
    Op *dequeue() {
      assert(apply_lock.is_locked());
      Mutex::Locker l(qlock);
      Op *o = q.front();
      q.pop_front();
      cond.Signal();
      return o;
    }
    void flush() {
      Mutex::Locker l(qlock);

      while (g_conf->filestore_blackhole)
	cond.Wait(qlock);  // wait forever

      // get max for journal _or_ op queues
      uint64_t seq = 0;
      if (!q.empty())
	seq = q.back()->op;
      if (!jq.empty() && jq.back() > seq)
	seq = jq.back();

      if (seq) {
	// everything prior to our watermark to drain through either/both queues
	while ((!q.empty() && q.front()->op <= seq) ||
	       (!jq.empty() && jq.front() <= seq))
	  cond.Wait(qlock);
      }
    }

    OpSequencer()
      : qlock("FileStore::OpSequencer::qlock", false, false),
	apply_lock("FileStore::OpSequencer::apply_lock", false, false) {}
    ~OpSequencer() {
      assert(q.empty());
    }

    const string& get_name() const {
      return parent->get_name();
    }
  };

  friend ostream& operator<<(ostream& out, const OpSequencer& s);

  Sequencer default_osr;
  deque<OpSequencer*> op_queue;
  uint64_t op_queue_len, op_queue_bytes;
  Cond op_throttle_cond;
  Finisher op_finisher;
  uint64_t next_finish;

  ThreadPool op_tp;
  struct OpWQ : public ThreadPool::WorkQueue<OpSequencer> {
    FileStore *store;
    OpWQ(FileStore *fs, time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<OpSequencer>("FileStore::OpWQ", timeout, suicide_timeout, tp), store(fs) {}

    bool _enqueue(OpSequencer *osr) {
      store->op_queue.push_back(osr);
      return true;
    }
    void _dequeue(OpSequencer *o) {
      assert(0);
    }
    bool _empty() {
      return store->op_queue.empty();
    }
    OpSequencer *_dequeue() {
      if (store->op_queue.empty())
	return NULL;
      OpSequencer *osr = store->op_queue.front();
      store->op_queue.pop_front();
      return osr;
    }
    void _process(OpSequencer *osr) {
      store->_do_op(osr);
    }
    void _process_finish(OpSequencer *osr) {
      store->_finish_op(osr);
    }
    void _clear() {
      assert(store->op_queue.empty());
    }
  } op_wq;

  void _do_op(OpSequencer *o);
  void _finish_op(OpSequencer *o);
  Op *build_op(list<Transaction*>& tls,
	       Context *onreadable, Context *onreadable_sync,
	       TrackedOpRef osd_op);
  void queue_op(OpSequencer *osr, Op *o);
  void op_queue_reserve_throttle(Op *o);
  void _op_queue_reserve_throttle(Op *o, const char *caller = 0);
  void _op_queue_release_throttle(Op *o);
  void _journaled_ahead(OpSequencer *osr, Op *o, Context *ondisk);
  friend class C_JournaledAhead;

  // flusher thread
  Cond flusher_cond;
  list<uint64_t> flusher_queue;
  int flusher_queue_len;
  void flusher_entry();
  struct FlusherThread : public Thread {
    FileStore *fs;
    FlusherThread(FileStore *f) : fs(f) {}
    void *entry() {
      fs->flusher_entry();
      return 0;
    }
  } flusher_thread;
  bool queue_flusher(int fd, uint64_t off, uint64_t len);

  int open_journal();


  PerfCounters *logger;

public:
  int lfn_find(coll_t cid, const hobject_t& oid, IndexedPath *path);
  int lfn_getxattr(coll_t cid, const hobject_t& oid, const char *name, void *val, size_t size);
  int lfn_setxattr(coll_t cid, const hobject_t& oid, const char *name, const void *val, size_t size);
  int lfn_removexattr(coll_t cid, const hobject_t& oid, const char *name);
  int lfn_listxattr(coll_t cid, const hobject_t& oid, char *names, size_t len);
  int lfn_truncate(coll_t cid, const hobject_t& oid, off_t length);
  int lfn_stat(coll_t cid, const hobject_t& oid, struct stat *buf);
  int lfn_open(coll_t cid, const hobject_t& oid, int flags, mode_t mode,
	       IndexedPath *path);
  int lfn_open(coll_t cid, const hobject_t& oid, int flags, mode_t mode,
	       IndexedPath *path, Index *index);
  int lfn_open(coll_t cid, const hobject_t& oid, int flags, mode_t mode);
  int lfn_open(coll_t cid, const hobject_t& oid, int flags);
  int lfn_link(coll_t c, coll_t cid, const hobject_t& o) ;
  int lfn_unlink(coll_t cid, const hobject_t& o, const SequencerPosition &spos);

 public:
  FileStore(const std::string &base, const std::string &jdev, const char *internal_name = "filestore", bool update_to=false);
  ~FileStore();

  int _test_fiemap();
  int _detect_fs();
  int _sanity_check_fs();
  
  bool test_mount_in_use();
  int write_version_stamp();
  int version_stamp_is_valid(uint32_t *version);
  int update_version_stamp();
  int read_op_seq(uint64_t *seq);
  int write_op_seq(int, uint64_t seq);
  int mount();
  int umount();
  int get_max_object_name_length();
  int mkfs();
  int mkjournal();

  int statfs(struct statfs *buf);

  int do_transactions(list<Transaction*> &tls, uint64_t op_seq);
  unsigned apply_transaction(Transaction& t, Context *ondisk=0);
  unsigned apply_transactions(list<Transaction*>& tls, Context *ondisk=0);
  unsigned _do_transaction(Transaction& t, uint64_t op_seq, int trans_num);

  int queue_transaction(Sequencer *osr, Transaction* t);
  int queue_transactions(Sequencer *osr, list<Transaction*>& tls,
			 Context *onreadable, Context *ondisk=0,
			 Context *onreadable_sync=0,
			 TrackedOpRef op = TrackedOpRef());

  /**
   * set replay guard xattr on given file
   *
   * This will ensure that we will not replay this (or any previous) operation
   * against this particular inode/object.
   *
   * @param fd open file descriptor for the file/object
   * @param spos sequencer position of the last operation we should not replay
   */
  void _set_replay_guard(int fd,
			 const SequencerPosition& spos,
			 const hobject_t *hoid=0,
			 bool in_progress=false);

  /// close a replay guard opened with in_progress=true
  void _close_replay_guard(int fd, const SequencerPosition& spos);

  /**
   * check replay guard xattr on given file
   *
   * Check the current position against any marker on the file that
   * indicates which operations have already been applied.  If the
   * current or a newer operation has been marked as applied, we
   * should not replay the current operation again.
   *
   * If we are not replaying the journal, we already return true.  It
   * is only on replay that we might return false, indicated that the
   * operation should not be performed (again).
   *
   * @param fd open fd on the file/object in question
   * @param spos sequencerposition for an operation we could apply/replay
   * @return 1 if we can apply (maybe replay) this operation, -1 if spos has already been applied, 0 if it was in progress
   */
  int _check_replay_guard(int fd, const SequencerPosition& spos);
  int _check_replay_guard(coll_t cid, const SequencerPosition& spos);
  int _check_replay_guard(coll_t cid, hobject_t oid, const SequencerPosition& pos);

  // ------------------
  // objects
  int pick_object_revision_lt(hobject_t& oid) {
    return 0;
  }
  bool exists(coll_t cid, const hobject_t& oid);
  int stat(coll_t cid, const hobject_t& oid, struct stat *st);
  int read(coll_t cid, const hobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);
  int fiemap(coll_t cid, const hobject_t& oid, uint64_t offset, size_t len, bufferlist& bl);

  int _touch(coll_t cid, const hobject_t& oid);
  int _write(coll_t cid, const hobject_t& oid, uint64_t offset, size_t len, const bufferlist& bl);
  int _zero(coll_t cid, const hobject_t& oid, uint64_t offset, size_t len);
  int _truncate(coll_t cid, const hobject_t& oid, uint64_t size);
  int _clone(coll_t cid, const hobject_t& oldoid, const hobject_t& newoid,
	     const SequencerPosition& spos);
  int _clone_range(coll_t cid, const hobject_t& oldoid, const hobject_t& newoid,
		   uint64_t srcoff, uint64_t len, uint64_t dstoff,
		   const SequencerPosition& spos);
  int _do_clone_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _do_copy_range(int from, int to, uint64_t srcoff, uint64_t len, uint64_t dstoff);
  int _remove(coll_t cid, const hobject_t& oid, const SequencerPosition &spos);

  void _start_sync();

  void start_sync();
  void start_sync(Context *onsafe);
  void sync();
  void _flush_op_queue();
  void flush();
  void sync_and_flush();

  int dump_journal(ostream& out);

  void set_fsid(uuid_d u) {
    fsid = u;
  }
  uuid_d get_fsid() { return fsid; }

  int snapshot(const string& name);

  // attrs
  int getattr(coll_t cid, const hobject_t& oid, const char *name, bufferptr &bp);
  int getattrs(coll_t cid, const hobject_t& oid, map<string,bufferptr>& aset, bool user_only = false);

  int _getattr(coll_t cid, const hobject_t& oid, const char *name, bufferptr& bp);
  int _getattrs(coll_t cid, const hobject_t& oid, map<string,bufferptr>& aset, bool user_only = false) ;
  int _getattr(const char *fn, const char *name, bufferptr& bp);
  int _getattrs(const char *fn, map<string,bufferptr>& aset, bool user_only = false);

  int _setattrs(coll_t cid, const hobject_t& oid, map<string,bufferptr>& aset,
		const SequencerPosition &spos);
  int _rmattr(coll_t cid, const hobject_t& oid, const char *name,
	      const SequencerPosition &spos);
  int _rmattrs(coll_t cid, const hobject_t& oid,
	       const SequencerPosition &spos);

  int collection_getattr(coll_t c, const char *name, void *value, size_t size);
  int collection_getattr(coll_t c, const char *name, bufferlist& bl);
  int collection_getattrs(coll_t cid, map<string,bufferptr> &aset);

  int _collection_setattr(coll_t c, const char *name, const void *value, size_t size);
  int _collection_rmattr(coll_t c, const char *name);
  int _collection_setattrs(coll_t cid, map<string,bufferptr> &aset);
  int _collection_remove_recursive(const coll_t &cid,
				   const SequencerPosition &spos);
  int _collection_rename(const coll_t &cid, const coll_t &ncid,
			 const SequencerPosition& spos);

  // collections
  int list_collections(vector<coll_t>& ls);
  int collection_version_current(coll_t c, uint32_t *version);
  int collection_stat(coll_t c, struct stat *st);
  bool collection_exists(coll_t c);
  bool collection_empty(coll_t c);
  int collection_list(coll_t c, vector<hobject_t>& o);
  int collection_list_partial(coll_t c, hobject_t start,
			      int min, int max, snapid_t snap,
			      vector<hobject_t> *ls, hobject_t *next);

  // omap (see ObjectStore.h for documentation)
  int omap_get(coll_t c, const hobject_t &hoid, bufferlist *header,
	       map<string, bufferlist> *out);
  int omap_get_header(coll_t c, const hobject_t &hoid, bufferlist *out);
  int omap_get_keys(coll_t c, const hobject_t &hoid, set<string> *keys);
  int omap_get_values(coll_t c, const hobject_t &hoid, const set<string> &keys,
		      map<string, bufferlist> *out);
  int omap_check_keys(coll_t c, const hobject_t &hoid, const set<string> &keys,
		      set<string> *out);
  ObjectMap::ObjectMapIterator get_omap_iterator(coll_t c, const hobject_t &hoid);

  int _create_collection(coll_t c);
  int _destroy_collection(coll_t c);
  int _collection_add(coll_t c, coll_t ocid, const hobject_t& o,
		      const SequencerPosition& spos);
  void dump_start(const std::string& file);
  void dump_stop();
  void dump_transactions(list<ObjectStore::Transaction*>& ls, uint64_t seq, OpSequencer *osr);

private:
  void _inject_failure();

  // omap
  int _omap_clear(coll_t cid, const hobject_t &hoid,
		  const SequencerPosition &spos);
  int _omap_setkeys(coll_t cid, const hobject_t &hoid,
		    const map<string, bufferlist> &aset,
		    const SequencerPosition &spos);
  int _omap_rmkeys(coll_t cid, const hobject_t &hoid, const set<string> &keys,
		   const SequencerPosition &spos);
  int _omap_setheader(coll_t cid, const hobject_t &hoid, const bufferlist &bl,
		      const SequencerPosition &spos);

  virtual const char** get_tracked_conf_keys() const;
  virtual void handle_conf_change(const struct md_config_t *conf,
			  const std::set <std::string> &changed);
  bool m_filestore_btrfs_clone_range;
  bool m_filestore_btrfs_snap;
  float m_filestore_commit_timeout;
  bool m_filestore_fiemap;
  bool m_filestore_flusher;
  bool m_filestore_fsync_flushes_journal_data;
  bool m_filestore_journal_parallel;
  bool m_filestore_journal_trailing;
  bool m_filestore_journal_writeahead;
  int m_filestore_fiemap_threshold;
  bool m_filestore_sync_flush;
  int m_filestore_flusher_max_fds;
  int m_filestore_flush_min;
  double m_filestore_max_sync_interval;
  double m_filestore_min_sync_interval;
  int do_update;
  bool m_journal_dio, m_journal_aio;
  std::string m_osd_rollback_to_cluster_snap;
  bool m_osd_use_stale_snap;
  int m_filestore_queue_max_ops;
  int m_filestore_queue_max_bytes;
  int m_filestore_queue_committing_max_ops;
  int m_filestore_queue_committing_max_bytes;
  bool m_filestore_do_dump;
  std::ofstream m_filestore_dump;
  JSONFormatter m_filestore_dump_fmt;
  atomic_t m_filestore_kill_at;
};

ostream& operator<<(ostream& out, const FileStore::OpSequencer& s);

#endif
