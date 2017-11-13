package zmq.test

import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context,Socket}

object RPCTestServer {
  def main(args : Array[String]) {
    val context = ZMQ.context(1)
    val socket = context.socket(ZMQ.REP)
    println ("starting")
    socket.bind ("tcp://*:5555")

    while (true) {
      val request = socket.recv(0)
      println ("Received request: [" + new String(request) + "]")

      try {
        Thread.sleep (1000)
      } catch  {
        case e: InterruptedException => e.printStackTrace()
      }
      socket.send(request, 0)
    }
  }
}

object RPCTestClient {
  def main(args : Array[String]) {
    val context = ZMQ.context(1)
    val socket = context.socket(ZMQ.REQ)

    println("Connecting to hello world server…")
    socket.connect ("tcp://localhost:5555")

    for ( rId <- 1 to 10)  {
      val request = args(0).getBytes()
      println("Sending request " + rId + "…") + request.toString
      socket.send(request, 0)
      val reply = socket.recv(0)
      println("Received reply " + rId + ": [" + new String(reply) + "]")
    }
  }
}