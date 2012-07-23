// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 New Dream Network/Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef OPREQUEST_H_
#define OPREQUEST_H_
#include <sstream>
#include <stdint.h>
#include <vector>

#include <include/utime.h>
#include "common/Mutex.h"
#include "include/xlist.h"
#include "msg/Message.h"
#include <tr1/memory>
#include "common/TrackedOp.h"
#include "osd/osd_types.h"

class OpRequest;
class OpHistory {
  set<pair<utime_t, const OpRequest *> > arrived;
  set<pair<double, const OpRequest *> > duration;
  void cleanup(utime_t now);

public:
  void insert(utime_t now, OpRequest *op);
  void dump_ops(utime_t now, Formatter *f);
};

class OpRequest;
typedef std::tr1::shared_ptr<OpRequest> OpRequestRef;
class OpTracker {
  class RemoveOnDelete {
    OpTracker *tracker;
  public:
    RemoveOnDelete(OpTracker *tracker) : tracker(tracker) {}
    void operator()(OpRequest *op);
  };
  friend class RemoveOnDelete;
  uint64_t seq;
  Mutex ops_in_flight_lock;
  xlist<OpRequest *> ops_in_flight;
  OpHistory history;

public:
  OpTracker() : seq(0), ops_in_flight_lock("OpTracker mutex") {}
  void dump_ops_in_flight(std::ostream& ss);
  void dump_historic_ops(std::ostream& ss);
  void register_inflight_op(xlist<OpRequest*>::item *i);
  void unregister_inflight_op(OpRequest *i);

  /**
   * Look for Ops which are too old, and insert warning
   * strings for each Op that is too old.
   *
   * @param warning_strings A vector<string> reference which is filled
   * with a warning string for each old Op.
   * @return True if there are any Ops to warn on, false otherwise.
   */
  bool check_ops_in_flight(std::vector<string> &warning_strings);
  void mark_event(OpRequest *op, const string &evt);
  void _mark_event(OpRequest *op, const string &evt, utime_t now);
  OpRequestRef create_request(Message *req);
};

/**
 * The OpRequest takes in a Message* and takes over a single reference
 * to it, which it puts() when destroyed.
 * OpRequest is itself ref-counted. The expectation is that you get a Message
 * you want to track, create an OpRequest with it, and then pass around that OpRequest
 * the way you used to pass around the Message.
 */
struct OpRequest : public TrackedOp {
  friend class OpTracker;
  friend class OpHistory;
  Message *request;
  xlist<OpRequest*>::item xitem;
  utime_t received_time;
  uint8_t warn_interval_multiplier;
  utime_t get_arrived() const {
    return received_time;
  }
  double get_duration() const {
    return events.size() ?
      (events.rbegin()->first - received_time) :
      0.0;
  }
  void dump(utime_t now, Formatter *f) const;
private:
  list<pair<utime_t, string> > events;
  Mutex lock;
  OpTracker *tracker;
  osd_reqid_t reqid;
  uint8_t hit_flag_points;
  uint8_t latest_flag_point;
  uint64_t seq;
  static const uint8_t flag_queued_for_pg=1 << 0;
  static const uint8_t flag_reached_pg =  1 << 1;
  static const uint8_t flag_delayed =     1 << 2;
  static const uint8_t flag_started =     1 << 3;
  static const uint8_t flag_sub_op_sent = 1 << 4;

  OpRequest(Message *req, OpTracker *tracker) :
    request(req), xitem(this),
    warn_interval_multiplier(1),
    lock("OpRequest::lock"),
    tracker(tracker),
    seq(0) {
    received_time = request->get_recv_stamp();
    tracker->register_inflight_op(&xitem);
  }
public:
  ~OpRequest() {
    assert(request);
    request->put();
  }

  bool been_queued_for_pg() { return hit_flag_points & flag_queued_for_pg; }
  bool been_reached_pg() { return hit_flag_points & flag_reached_pg; }
  bool been_delayed() { return hit_flag_points & flag_delayed; }
  bool been_started() { return hit_flag_points & flag_started; }
  bool been_sub_op_sent() { return hit_flag_points & flag_sub_op_sent; }
  bool currently_queued_for_pg() { return latest_flag_point & flag_queued_for_pg; }
  bool currently_reached_pg() { return latest_flag_point & flag_reached_pg; }
  bool currently_delayed() { return latest_flag_point & flag_delayed; }
  bool currently_started() { return latest_flag_point & flag_started; }
  bool currently_sub_op_sent() { return latest_flag_point & flag_sub_op_sent; }

  const char *state_string() const {
    switch(latest_flag_point) {
    case flag_queued_for_pg: return "queued for pg";
    case flag_reached_pg: return "reached pg";
    case flag_delayed: return "delayed";
    case flag_started: return "started";
    case flag_sub_op_sent: return "waiting for sub ops";
    default: break;
    }
    return "no flag points reached";
  }

  void mark_queued_for_pg() {
    mark_event("queued_for_pg");
    hit_flag_points |= flag_queued_for_pg;
    latest_flag_point = flag_queued_for_pg;
  }
  void mark_reached_pg() {
    mark_event("reached_pg");
    hit_flag_points |= flag_reached_pg;
    latest_flag_point = flag_reached_pg;
  }
  void mark_delayed() {
    hit_flag_points |= flag_delayed;
    latest_flag_point = flag_delayed;
  }
  void mark_started() {
    mark_event("started");
    hit_flag_points |= flag_started;
    latest_flag_point = flag_started;
  }
  void mark_sub_op_sent() {
    mark_event("sub_op_sent");
    hit_flag_points |= flag_sub_op_sent;
    latest_flag_point = flag_sub_op_sent;
  }

  void mark_event(const string &event);
  osd_reqid_t get_reqid() const {
    return reqid;
  }
};

#endif /* OPREQUEST_H_ */
