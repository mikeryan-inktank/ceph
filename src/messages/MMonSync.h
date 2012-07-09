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
#ifndef CEPH_MMONSYNC_H
#define CEPH_MMONSYNC_H

#include "msg/Message.h"

class MMonSync : public Message
{
  static const int HEAD_VERSION = 1;
  static const int COMPAT_VERSION = 1;

public:
  /**
  * Operation types
  */
  enum {
    /**
    * Start synchronization request
    */
    OP_START		= 1,
    /**
    * Message contains chunk to be applied to the requester's store
    */
    OP_CHUNK		= 2,
    /**
    * Acknowledgement of the last received chunk
    */
    OP_CHUNK_ACK	= 3,
    /**
     * Request the Leader to temporarily disable trimmig
     */
    OP_TRIM_DISABLE	= 4,
    /**
     * Let the Leader know that we are okay if trim is enabled
     */
    OP_TRIM_ENABLE	= 5,
    /**
     * Acknowledgment of trim disable request
     *
     * @note for posteriority, we don't need to acknowledge trim enable ops,
     *	     simply because those should be taken simply as mere courtesy; the
     *	     leader would eventually re-enable trimming, so sending a trim
     *	     enable operation just means that he can enable them earlier if he
     *	     so desires, and there is no need for the leader to ack that
     *	     message, nor for the sender to wait for a reply.
     */
    OP_TRIM_DISABLE_ACK	= 6,
  };

  /**
  * Chunk is the last available
  */
  const static int FLAG_LAST	      = 0x01;
  /**
  * The chunk's bufferlist is an encoded transaction
  */
  const static int FLAG_ENCODED_TX    = 0x02;
  /**
   * Renew a trim disable
   */
  const static int FLAG_RENEW	      = 0x04;
  /**
   * Operation/Request was denied
   */
  const static int FLAG_DENIED	      = 0x08;

  /**
  * Obtain a string corresponding to the operation type @p op
  *
  * @param op Operation type
  * @returns A string
  */
  static const char *get_opname(int op) {
    switch (op) {
    case OP_START: return "start";
    case OP_CHUNK: return "chunk";
    case OP_CHUNK_ACK: return "chunk_ack";
    case OP_TRIM_ENABLE: return "trim_enable";
    case OP_TRIM_DISABLE: return "trim_disable";
    case OP_TRIM_ACK: return "trim_ack";
    default: assert("unknown op type"); return NULL;
    }
  }

  uint32_t op;
  uint8_t flags;
  bufferlist chunk_bl;
  version_t version;

  MMonSync(uint32_t _op, bufferlist bl, uint8_t _flags = 0) 
    : op(_op), flags(_flags), chunk_bl(bl), version(0)
  { }

  MMonSync(uint32_t _op)
    : op(_op), flags(0), version(0)
  { }

  /**
  * Obtain this message type's name */
  const char *get_type_name() const { return "mon_sync"; }

  /**
  * Print this message in a pretty format to @p out
  *
  * @param out The output stream to output to
  */
  void print(ostream& out) const {
    out << "mon_sync( " << get_opname(op);

    if (version > 0)
      out << " v " << version;

    if (flags) {
      out << " flags( ";
      if (flags & FLAG_LAST)
	out << "last ";
      if (flags & FLAG_ENCODED_TX)
	out << "encoded_tx ";
      out << ")";
    }

    if (chunk_bl.length())
      out << " bl " << chunk_bl.length() << " bytes";

    out << " )";	
  }

  /**
  * Encode this message into the Message's payload
  */
  void encode_payload(uint64_t features) {
    ::encode(op, payload);
    ::encode(flags, payload);
    ::encode(chunk_bl, payload);
    ::encode(version, payload);
  }

  /**
  * Decode the message's payload into this message
  */
  void decode_payload() {
    bufferlist::iterator p = payload.begin();
    ::decode(op, p);
    ::decode(flags, p);
    ::decode(chunk_bl, p);
    ::decode(version, p);
  }
};

#endif /* CEPH_MMONSYNC_H */
