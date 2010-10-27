/*
 * LibCassandra
 * Copyright (C) 2010 Padraig O'Sullivan
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <string>
#include <set>
#include <sstream>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "libgenthrift/Cassandra.h"

#include <boost/shared_ptr.hpp>

#include "libcassandra/cassandra.h"
#include "libcassandra/cassandra_factory.h"

using namespace libcassandra;
using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace org::apache::cassandra;
using namespace boost;


CassandraFactory::CassandraFactory(const string& server_list)
  :
    url(server_list),
    host(),
    port(0),
    conn_timeout(-1),
    recv_timeout(-1),
    send_timeout(-1)
{
  /* get the host name from the server list string */
  string::size_type pos= server_list.find_first_of(':');
  host= server_list.substr(0, pos);
  /* get the port from the server list string */
  string tmp_port= server_list.substr(pos + 1);
  /* convert to integer */
  istringstream int_stream(tmp_port);
  int_stream >> port;
}

CassandraFactory::CassandraFactory(const string& in_host, int in_port)
  :
    url(),
    host(in_host),
    port(in_port),
    conn_timeout(-1),
    recv_timeout(-1),
    send_timeout(-1)
{
  url.append(host);
  url.append(":");
  ostringstream port_str;
  port_str << port;
  url.append(port_str.str());
}

CassandraFactory::CassandraFactory(const string &in_host, int in_port, int in_timeout)
  :
    host(in_host),
    port(in_port),
    conn_timeout(in_timeout),
    recv_timeout(in_timeout),
    send_timeout(in_timeout)
    url(),
    host(in_host),
    port(in_port),
    conn_timeout(in_timeout),
    recv_timeout(in_timeout),
    send_timeout(in_timeout)
{
  url.append(host);
  url.append(":");
  ostringstream port_str;
  port_str << port;
  url.append(port_str.str());
}

CassandraFactory::~CassandraFactory() {}


boost::shared_ptr<Cassandra> CassandraFactory::create()
{
  CassandraClient *thrift_client= createThriftClient(host, port, conn_timeout, recv_timeout, send_timeout);
  boost::shared_ptr<Cassandra> ret(new Cassandra(thrift_client, host, port));
  return ret;
}


boost::shared_ptr<Cassandra> CassandraFactory::create(const string& keyspace)
{
  CassandraClient *thrift_client= createThriftClient(host, port, conn_timeout, recv_timeout, send_timeout);
  boost::shared_ptr<Cassandra> ret(new Cassandra(thrift_client, host, port, keyspace));
  return ret;
}


CassandraClient *CassandraFactory::createThriftClient(const string &in_host,
                                                      int in_port,
                                                      int in_conn_timeout,
                                                      int in_recv_timeout,
                                                      int in_send_timeout)
{
  boost::shared_ptr<TSocket> socket(new TSocket(in_host, in_port));

  if (in_conn_timeout >= 0) {
    socket->setConnTimeout(in_conn_timeout);
  }
  if (in_recv_timeout >= 0) {
    socket->setRecvTimeout(in_recv_timeout);
  }
  if (in_send_timeout >= 0) {
    socket->setSendTimeout(in_send_timeout);
  }

  }
  if (in_send_timeout >= 0) {
    socket->setSendTimeout(in_send_timeout);
  }

  boost::shared_ptr<TTransport> transport = boost::shared_ptr<TTransport>(new TFramedTransport(socket));
  boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  transport->open(); /* throws an exception */

  CassandraClient *client= new(std::nothrow) CassandraClient(protocol);

  return client;
}


const string &CassandraFactory::getURL() const
{
  return url;
}


const string &CassandraFactory::getHost() const
{
  return host;
}


int CassandraFactory::getPort() const
{
  return port;
}
