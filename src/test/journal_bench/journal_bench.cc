#include <stdlib.h>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>

#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "global/global_init.h"
#include "common/config.h"
#include "common/Finisher.h"
#include "os/FileJournal.h"
#include "include/Context.h"
#include "common/Mutex.h"
#include "common/safe_io.h"
#include "test/bench/detailed_stat_collector.h"
#include "common/Semaphore.h"

namespace po = boost::program_options;

Finisher *finisher;
Cond sync_cond;
char path[200];
uuid_d fsid;
bool directio = false;
bool aio = false;

static utime_t cur_time()
{
  struct timeval tv;
  gettimeofday(&tv, 0);
  return utime_t(&tv);
}

// ----
Cond cond;
Mutex lock("lock");
bool done;

void wait()
{
  lock.Lock();
  while (!done)
    cond.Wait(lock);
  lock.Unlock();
}

// ----
class C_Sync {
public:
  Cond cond;
  Mutex lock;
  bool done;
  C_SafeCond *c;

  C_Sync()
    : lock("C_Sync::lock"), done(false) {
    c = new C_SafeCond(&lock, &cond, &done);
  }
  ~C_Sync() {
    lock.Lock();
    //cout << "wait" << std::endl;
    while (!done)
      cond.Wait(lock);
    //cout << "waited" << std::endl;
    lock.Unlock();
  }
};

class C_LogJournaled : public Context {
  utime_t time;
  DetailedStatCollector::Aggregator *agg;
  Semaphore *sem;

public:
  C_LogJournaled(utime_t t, DetailedStatCollector::Aggregator *a, Semaphore *s) : time(t), agg(a), sem(s)
  {
  }

  void finish(int r) {
    sem->Put();
    agg->add(
      DetailedStatCollector::Op(
        "journaled",
        time,
        cur_time() - time,
        100,//msg->bl.length(),
        0));
    if (cur_time() - agg->get_last() >= 1)
      dump();
  }

  void dump() {
    JSONFormatter f;
    f.open_object_section("throughput");
    agg->dump(&f);
    f.close_section();
    f.flush(std::cout);
    std::cout << std::endl;
  }
};


unsigned size_mb = 200;

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  desc.add_options()
    ("help", "produce help message")
    ("role", po::value<string>()->default_value("server"),
     "server or client")
    ("server-addr", po::value<string>()->default_value("127.0.0.1:12345"),
     "server addr")
    ("dio", po::value<bool>()->default_value("false"),
     "direct io")
    ("aio", po::value<bool>()->default_value("false"),
     "async io")
    ("debug-to-stderr", po::value<bool>()->default_value(false),
     "send debug to stderr")
    ("max-in-flight", po::value<unsigned>()->default_value(10),
     "max uncommitted entries")
    ("size", po::value<unsigned>()->default_value(4<<20),
     "size to send")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::store(
    parsed,
    vm);
  po::notify(vm);

  /*
  CephInitParameters iparams(CEPH_ENTITY_TYPE_CLIENT);

  CephContext *cct = common_preinit(iparams, CODE_ENVIRONMENT_LIBRARY, 0);
  cct->_conf->subsys.set_log_level(
    ceph_subsys_ms,
    vm["debug-ms"].as<unsigned>());
  if (vm["debug-to-stderr"].as<bool>())
    cct->_conf->set_val("log_to_stderr", "1");
  if (!vm["disable-nagle"].as<bool>())
    cct->_conf->set_val("ms_tcp_nodelay", "false");
  cct->_conf->apply_changes(NULL);
  */

  vector<const char *> ceph_options, def_args;
  vector<string> ceph_option_strings = po::collect_unrecognized(
    parsed.options, po::include_positional);
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  // vector<const char*> args;
  // argv_to_vec(argc, (const char **)argv, args);
  // global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  // common_init_finish(g_ceph_context);

  DetailedStatCollector::Aggregator agg;

  char mb[10];
  sprintf(mb, "%d", size_mb);
  g_ceph_context->_conf->set_val("osd_journal_size", mb);
  g_ceph_context->_conf->apply_changes(NULL);

  finisher = new Finisher(g_ceph_context);
  
  srand(getpid()+time(0));
  snprintf(path, sizeof(path), "/tmp/test_filejournal.tmp.%d", rand());

  finisher->start();

  cout << "DIRECTIO OFF  AIO OFF" << std::endl;
  directio = false;
  aio = false;

  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio, aio);
  j.create();
  j.make_writeable();

  Semaphore sem; 
  for (unsigned i = 0; i < vm["max-in-flight"].as<unsigned>(); ++i) sem.Put();

  for (uint64_t seq = 0; ; ++seq) {
    bufferlist bl;
    while (bl.length() < size_mb*4) {
      char foo[1024*1024];
      memset(foo, 1, sizeof(foo));
      bl.append(foo, sizeof(foo));
    }

    if (seq > 30 && (seq % 30) == 0)
      j.committed_thru(seq - 10);

    sem.Get();
    j.submit_entry(seq, bl, 0, new C_LogJournaled(cur_time(), &agg, &sem));
    j.commit_start();
  }
  wait();

  j.close();


  int r = 0;
  /*
  int r = RUN_ALL_TESTS();
  if (r >= 0) {
    cout << "DIRECTIO ON  AIO OFF" << std::endl;
    directio = true;
    r = RUN_ALL_TESTS();

    if (r >= 0) {
      cout << "DIRECTIO ON  AIO ON" << std::endl;
      aio = true;
      r = RUN_ALL_TESTS();
    }
  }
  */
  
  finisher->stop();

  unlink(path);
  
  return r;
}

/*
TEST(TestFileJournal, Create) {
  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio, aio);
  ASSERT_EQ(0, j.create());
}

TEST(TestFileJournal, WriteSmall) {
  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio, aio);
  ASSERT_EQ(0, j.create());
  j.make_writeable();

  bufferlist bl;
  bl.append("small");
  j.submit_entry(1, bl, 0, new C_SafeCond(&lock, &cond, &done));
  wait();

  j.close();
}

TEST(TestFileJournal, WriteBig) {
  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio, aio);
  ASSERT_EQ(0, j.create());
  j.make_writeable();

  bufferlist bl;
  while (bl.length() < size_mb*1000/2) {
    char foo[1024*1024];
    memset(foo, 1, sizeof(foo));
    bl.append(foo, sizeof(foo));
  }
  j.submit_entry(1, bl, 0, new C_SafeCond(&lock, &cond, &done));
  wait();

  j.close();
}

TEST(TestFileJournal, WriteMany) {
  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio, aio);
  ASSERT_EQ(0, j.create());
  j.make_writeable();

  C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&lock, &cond, &done));
  
  bufferlist bl;
  bl.append("small");
  uint64_t seq = 1;
  for (int i=0; i<100; i++) {
    bl.append("small");
    j.submit_entry(seq++, bl, 0, gb.new_sub());
  }

  gb.activate();

  wait();

  j.close();
}

TEST(TestFileJournal, ReplaySmall) {
  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio, aio);
  ASSERT_EQ(0, j.create());
  j.make_writeable();
  
  C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&lock, &cond, &done));
  
  bufferlist bl;
  bl.append("small");
  j.submit_entry(1, bl, 0, gb.new_sub());
  bl.append("small");
  j.submit_entry(2, bl, 0, gb.new_sub());
  bl.append("small");
  j.submit_entry(3, bl, 0, gb.new_sub());
  gb.activate();
  wait();

  j.close();

  j.open(1);

  bufferlist inbl;
  string v;
  uint64_t seq = 0;
  ASSERT_EQ(true, j.read_entry(inbl, seq));
  ASSERT_EQ(seq, 2ull);
  inbl.copy(0, inbl.length(), v);
  ASSERT_EQ("small", v);
  inbl.clear();
  v.clear();

  ASSERT_EQ(true, j.read_entry(inbl, seq));
  ASSERT_EQ(seq, 3ull);
  inbl.copy(0, inbl.length(), v);
  ASSERT_EQ("small", v);
  inbl.clear();
  v.clear();

  ASSERT_TRUE(!j.read_entry(inbl, seq));

  j.make_writeable();
  j.close();
}

TEST(TestFileJournal, ReplayCorrupt) {
  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio, aio);
  ASSERT_EQ(0, j.create());
  j.make_writeable();
  
  C_GatherBuilder gb(g_ceph_context, new C_SafeCond(&lock, &cond, &done));
  
  const char *needle =    "i am a needle";
  const char *newneedle = "in a haystack";
  bufferlist bl;
  bl.append(needle);
  j.submit_entry(1, bl, 0, gb.new_sub());
  bl.append(needle);
  j.submit_entry(2, bl, 0, gb.new_sub());
  bl.append(needle);
  j.submit_entry(3, bl, 0, gb.new_sub());
  bl.append(needle);
  j.submit_entry(4, bl, 0, gb.new_sub());
  gb.activate();
  wait();

  j.close();

  cout << "corrupting journal" << std::endl;
  char buf[1024*128];
  int fd = open(path, O_RDONLY);
  ASSERT_GE(fd, 0);
  int r = safe_read_exact(fd, buf, sizeof(buf));
  ASSERT_EQ(0, r);
  int n = 0;
  for (unsigned o=0; o < sizeof(buf) - strlen(needle); o++) {
    if (memcmp(buf+o, needle, strlen(needle)) == 0) {
      if (n >= 2) {
	cout << "replacing at offset " << o << std::endl;
	memcpy(buf+o, newneedle, strlen(newneedle));
      } else {
	cout << "leaving at offset " << o << std::endl;
      }
      n++;
    }
  }
  ASSERT_EQ(n, 4);
  close(fd);
  fd = open(path, O_WRONLY);
  ASSERT_GE(fd, 0);
  r = safe_write(fd, buf, sizeof(buf));
  ASSERT_EQ(r, 0);
  close(fd);

  j.open(1);

  bufferlist inbl;
  string v;
  uint64_t seq = 0;
  ASSERT_EQ(true, j.read_entry(inbl, seq));
  ASSERT_EQ(seq, 2ull);
  inbl.copy(0, inbl.length(), v);
  ASSERT_EQ(needle, v);
  inbl.clear();
  v.clear();
  ASSERT_TRUE(!j.read_entry(inbl, seq));

  j.make_writeable();
  j.close();
}

TEST(TestFileJournal, WriteTrim) {
  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio, aio);
  ASSERT_EQ(0, j.create());
  j.make_writeable();

  list<C_Sync*> ls;
  
  bufferlist bl;
  char foo[1024*1024];
  memset(foo, 1, sizeof(foo));

  uint64_t seq = 1, committed = 0;

  for (unsigned i=0; i<size_mb*2; i++) {
    bl.clear();
    bl.push_back(buffer::copy(foo, sizeof(foo)));
    bl.zero();
    ls.push_back(new C_Sync);
    j.submit_entry(seq++, bl, 0, ls.back()->c);

    while (ls.size() > size_mb/2) {
      delete ls.front();
      ls.pop_front();
      committed++;
      j.committed_thru(committed);
    }
  }

  while (ls.size()) {
    delete ls.front();
    ls.pop_front();
    j.committed_thru(committed);
  }

  j.close();
}

TEST(TestFileJournal, WriteTrimSmall) {
  fsid.generate_random();
  FileJournal j(fsid, finisher, &sync_cond, path, directio);
  ASSERT_EQ(0, j.create());
  j.make_writeable();

  list<C_Sync*> ls;
  
  bufferlist bl;
  char foo[1024*1024];
  memset(foo, 1, sizeof(foo));

  uint64_t seq = 1, committed = 0;

  for (unsigned i=0; i<size_mb*2; i++) {
    bl.clear();
    for (int k=0; k<128; k++)
      bl.push_back(buffer::copy(foo, sizeof(foo) / 128));
    bl.zero();
    ls.push_back(new C_Sync);
    j.submit_entry(seq++, bl, 0, ls.back()->c);

    while (ls.size() > size_mb/2) {
      delete ls.front();
      ls.pop_front();
      committed++;
      j.committed_thru(committed);
    }
  }

  while (ls.size()) {
    delete ls.front();
    ls.pop_front();
    j.committed_thru(committed);
  }

  j.close();
}
*/
