# a prototype of paxos algorithm

import Queue
import threading
import time
import os
import copy
import random

class MsgNode:
   id = 0 
   recv_q = None
   def __init__(self):
      self.recv_q = Queue.Queue(30)
      self.id = 0

   def send(self, msg):
      g_msg_router.send(msg)

   def recv(self, timeout=None):
      return self.recv_q.get(True, timeout)

class Proposer(MsgNode, threading.Thread):
   val = None     #the value to be proposed, if possible
   _last_proposal_num = MsgNode.id
   _last_proposal_val = None
   def __init__(self, id, val):
      threading.Thread.__init__(self)
      MsgNode.__init__(self)
      self.id = id
      self.val = val
      self._last_proposal_num = id
      self._last_proposal_val = None

   def propose(self):
      #a proposer selects a proposal number n and sends a prepare
      #request with number n to a majority of acceptors
      print "proposer", self.id, "start propose", self._last_proposal_num
      req = PrepareReq()
      self._last_proposal_num = self._last_proposal_num + NUM_OF_PROPOSER
      req.src = self.id
      req.proposal_num = self._last_proposal_num
      g_msg_router.broadcast(req)
    
   def accept(self, prep_rsp_list):
      #sends an accept request to each of those acceptors for a proposal
      #numbered n with a value v, where v is the value of the highest 
      #numbered proposal among the responses, or is any value if responses
      #reported no proposals.
      acc_req_list = []
      _accepted_val = None
      _accepted_num = 0
      for i in prep_rsp_list:
         if i.accepted_num > _accepted_num:
            _accepted_num = i.accepted_num
            _accepted_val = i.accepted_val
      for i in prep_rsp_list:
         acpt = AcceptReq()
         acpt.src = self.id
         acpt.dest = i.src
         acpt.proposal_num = self._last_proposal_num
         if _accepted_num == 0:
            acpt.proposal_val = self.val
         else:
            acpt.proposal_val = _accepted_val
         self._last_proposal_val = acpt.proposal_val
         g_msg_router.send(acpt)

   def run(self):
      _prep_rsp_list = []     #the list of prepare response received from acceptors
      _acc_rsp_list = []      #the list of accept response received from acceptors
      while True:
         try:
            msg = self.recv(10)
            time.sleep(random.randint(0,3))  #introduce some concurrency
            if isinstance(msg, PrepareRsp):
               print "proposer", self.id, "got", msg
               if msg.promise_num==self._last_proposal_num:
                  _prep_rsp_list.append(msg)
                  if len(_prep_rsp_list) >= QUOROM_OF_ACCEPTOR:
                     #if the proposer receives a response to its prepare requests
                     #(numbered n) from a majority of acceptors, then
                     #accept it
                     print "proposer", self.id, "start accept", self._last_proposal_num 
                     self.accept(_prep_rsp_list)
               else:
                  #if the proposal number in the response does not match n,
                  #ignore the msg.(and will eventually lead to a re-proposal operation??)
                  pass
            elif isinstance(msg, PrepareReq):
               #ignore
               pass
            elif isinstance(msg, AcceptRsp):
               if msg.proposal_num==self._last_proposal_num:
                  _acc_rsp_list.append(msg)
                  if len(_acc_rsp_list) >= QUOROM_OF_ACCEPTOR:
                     print "================consensus reached!!", self._last_proposal_val
                     break     #stop propose loop
            elif isinstance(msg, AcceptReq):
               #ignore
               pass
         except Queue.Empty:
            #timeout, proposal again with a new proposal number
            self.propose()
 
   def __str__(self):
      return " ".join(("P", self.id))

class Acceptor(MsgNode, threading.Thread):
   h_n = 0  #highest proposal number ever promised in prepare rsponse
   a_n = 0  #highest proposal number accepted
   a_v = None  #the value accepted
   def __init__(self, id):
      threading.Thread.__init__(self)
      MsgNode.__init__(self)
      self.id = id

   def run(self):
      while True:
         msg = self.recv()
         if isinstance(msg, PrepareRsp):
            #ignore
            pass
         elif isinstance(msg, PrepareReq):
            print "acceptor", self.id, "got", msg
            print "acceptor", self.id, "status", self
            #if an acceptor receives a prepare request with number n greater than
            #that of any prepare request to which it has already responded, then
            #it responds to the request with a promise not to accept any more
            #proposals numbered less than n and with highest-numbered proposal(
            #if any) that it has accepted.
            if msg.proposal_num > self.h_n:
               rsp = PrepareRsp()
               rsp.src = msg.dest
               rsp.dest = msg.src
               rsp.promise_num = msg.proposal_num
               self.h_n = msg.proposal_num
               rsp.accepted_num = self.a_n
               rsp.accepted_val = self.a_v
               g_msg_router.send(rsp)
               print "acceptor", self.id, "send", rsp
               self.h_n = msg.proposal_num
         elif isinstance(msg, AcceptRsp):
            #ignore
            pass
         elif isinstance(msg, AcceptReq):
            print "acceptor", self.id, "got", msg
            print "acceptor", self.id, "status", self
            #if an acceptor receives an accept request for a proposal numbered n,
            #it accepts the proposal unless it has already responded to a prepare
            #request having a number greater than n.
            if msg.proposal_num < self.h_n:
               #ignore the message
               pass
            else:
               rsp = AcceptRsp()
               rsp.src = msg.dest
               rsp.dest = msg.src
               rsp.proposal_num = msg.proposal_num
               self.a_n = msg.proposal_num
               self.a_v = msg.proposal_val
               g_msg_router.send(rsp)
               print "acceptor", self.id, "send", rsp
 
   def __str__(self):
      return " ".join(("h_n", str(self.h_n), 
                       "a_n", str(self.a_n),
                       "a_v", str(self.a_v)))

class Message:
   src = 0
   dest = 0

class PrepareReq(Message):
   proposal_num = 0

   def __str__(self):
      return " ".join(("PrepareReq[src:",str(self.src),"dest:"+str(self.dest),
                       "proposal_num:",str(self.proposal_num),"]"))
      
class PrepareRsp(Message):
   promise_num = 0
   accepted_num = 0
   accepted_val = 0
   
   def __str__(self):
      return " ".join(("PrepareRsp[src:",str(self.src),"dest:",str(self.dest),
                       "promise_num:",str(self.promise_num),
                       "accepted_num:",str(self.accepted_num),
                       "accepted_val:",str(self.accepted_val), "]"))

class AcceptReq(Message):
   proposal_num = 0
   proposal_val = None 

   def __str__(self):
      return " ".join(("AcceptReq[src:",str(self.src),"dest:"+str(self.dest),
                       "proposal_num:",str(self.proposal_num),
                       "proposal_val:",str(self.proposal_val), "]"))

class AcceptRsp(Message):
   proposal_num = 0

   def __str__(self):
      return " ".join(("AcceptRsp[src:",str(self.src),"dest:",str(self.dest),
                       "proposal_num:",str(self.proposal_num), "]"))

class MsgRouter:
   node_list = []
   def register_node(self, node):
      self.node_list.append(node)

   def send(self, msg):
      dest = msg.dest
      for n in self.node_list:
         if dest==n.id:
            n.recv_q.put(msg)
            return
      raise "Node Not Found!!"

   def broadcast(self, msg):
      for n in self.node_list:
         tmp = copy.deepcopy(msg)
         tmp.dest = n.id
         n.recv_q.put(tmp)
   

# setup proposer and acceptor

# connect all node.
g_p_list = []
NUM_OF_PROPOSER = 3 
g_a_list = []
NUM_OF_ACCEPTOR = 3
QUOROM_OF_ACCEPTOR = 2
g_msg_router = MsgRouter()  

def init_env():
   for i in xrange(0, NUM_OF_ACCEPTOR):
      tmp = Acceptor(i)
      tmp.start()
      g_a_list.append(tmp)
      g_msg_router.register_node(tmp)

   for i in xrange(10, 10+NUM_OF_PROPOSER):
      tmp = Proposer(i, "hello consensus!!"+str(i))
      tmp.start()
      g_p_list.append(tmp)
      g_msg_router.register_node(tmp)


def paxos_consensus():
   init_env()

# main entrance  
if __name__ == "__main__":
   paxos_consensus()
   while True:
      try:
         time.sleep(3)
      except:
         os._exit(0)
