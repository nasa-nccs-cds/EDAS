package zmq.test

import org.zeromq.ZMQ
import org.zeromq.ZMQ.{Context,Socket}

object PushPullTestServer {
  def main(args : Array[String]) {
    val context = ZMQ.context(1)
    val push_socket = context.socket(ZMQ.PUSH)
    val pull_socket = context.socket(ZMQ.PULL)
    println ("starting")
    pull_socket.bind ("tcp://*:5555")
    push_socket.bind ("tcp://*:5556")

    while (true) {
      val request = pull_socket.recv(0)
      println ("Received request: [" + new String(request) + "]")

      try {
        Thread.sleep (1000)
      } catch  {
        case e: InterruptedException => e.printStackTrace()
      }
      push_socket.send(request, 0)
      push_socket.send(request, 0)
      push_socket.send(request, 0)
    }
  }
}

object PushPullTestClient {
  def main(args : Array[String]) {
    val context = ZMQ.context(1)
    val push_socket = context.socket(ZMQ.PUSH)
    val pull_socket = context.socket(ZMQ.PULL)
    val client_id = args(0)

    println("Connecting to hello world server…")
    push_socket.connect ("tcp://localhost:5555")
    pull_socket.connect ("tcp://localhost:5556")

    for ( rId <- 1 to 10) {
      val request = client_id.getBytes()
      println("Sending request[" + rId.toString + "]: " + client_id)
      push_socket.send(request, 0)
    }
    while( true ) {
      val reply = pull_socket.recv(0)
      println("Received reply( id = " + client_id + " ): [" + new String(reply) + "]")
    }
  }
}