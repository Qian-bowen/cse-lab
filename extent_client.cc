// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// extent_client::extent_client()
// {
//   // es = new extent_server();
// }

extent_client::extent_client(std::string extent_dst)
{
  sockaddr_in dstsock;
  make_sockaddr(extent_dst.c_str(), &dstsock);

  cl = new rpcc(dstsock);
  if (cl->bind() < 0) {
    printf("rpc client: call bind\n");
  }
}

extent_client::~extent_client()
{
    delete cl;
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id)
{
  // printf("extent_client::create\n");
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::create,type,id);
  // ret = es->create(type, id);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  // printf("extent_client::get\n");
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::get,eid,buf);
  // ret = es->get(eid, buf);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  // printf("extent_client::getattr\n");
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::getattr,eid,attr);
  // ret = es->getattr(eid, attr);
  return ret;
}

extent_protocol::status
extent_client::setattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr attr)
{
  // printf("extent_client setattr\n");
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  ret = cl->call(extent_protocol::setattr,eid,attr,r);
  // ret = es->setattr(eid, attr);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  // printf("extent_client put\n");
  extent_protocol::status ret = extent_protocol::OK;
  int r,r1;
  ret = cl->call(extent_protocol::put,eid,buf,r);
  // ret = es->put(eid, buf, r);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  // printf("extent_client remove\n");
  extent_protocol::status ret = extent_protocol::OK;
  int r;
  ret = cl->call(extent_protocol::remove,eid,r);
  // ret = es->remove(eid, r);
  return ret;
}


