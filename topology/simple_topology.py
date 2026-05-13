# type: ignore[reportUnknownVariableType,reportUnknownMemberType]
# ^^ Mininet 库无类型存根 — suppress 所有 Unknown 诊断

"""
Mininet 简易测试拓扑 — 端到端验证

拓扑：4 交换机 环形拓扑 + 每交换机 2 主机

   h1 --- s1 --- s2 --- h3
          |       |
   h2 --- +       + --- h4
          |       |
          s4 ---- s3
           |       |
          h8      h6,h5

用途：验证控制器消息队列流水线
  - 发送 ping / iperf 流量
  - 检查 Worker 的 east_queue / west_queue 是否正确接收消息
"""

from mininet.net import Mininet
from mininet.node import RemoteController, OVSSwitch
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink


def simple_topology():
    """创建 4 交换机环形拓扑"""
    net = Mininet(
        controller=RemoteController,
        switch=OVSSwitch,
        link=TCLink,
        autoSetMacs=True,
    )

    info("*** Adding controller\n")
    c0 = net.addController("c0", controller=RemoteController, ip="127.0.0.1", port=6653)

    info("*** Adding switches\n")
    s1 = net.addSwitch("s1", protocols="OpenFlow13")
    s2 = net.addSwitch("s2", protocols="OpenFlow13")
    s3 = net.addSwitch("s3", protocols="OpenFlow13")
    s4 = net.addSwitch("s4", protocols="OpenFlow13")

    info("*** Adding hosts\n")
    h1 = net.addHost("h1", ip="10.0.0.1/24")
    h2 = net.addHost("h2", ip="10.0.0.2/24")
    h3 = net.addHost("h3", ip="10.0.0.3/24")
    h4 = net.addHost("h4", ip="10.0.0.4/24")
    h5 = net.addHost("h5", ip="10.0.0.5/24")
    h6 = net.addHost("h6", ip="10.0.0.6/24")
    h7 = net.addHost("h7", ip="10.0.0.7/24")
    h8 = net.addHost("h8", ip="10.0.0.8/24")

    info("*** Creating links\n")
    # 环形主干链路
    net.addLink(s1, s2)
    net.addLink(s2, s3)
    net.addLink(s3, s4)
    net.addLink(s4, s1)

    # 主机接入链路
    net.addLink(h1, s1)
    net.addLink(h2, s1)
    net.addLink(h3, s2)
    net.addLink(h4, s2)
    net.addLink(h5, s3)
    net.addLink(h6, s3)
    net.addLink(h7, s4)
    net.addLink(h8, s4)

    info("*** Starting network\n")
    net.start()

    info("*** Testing connectivity\n")
    net.pingAll()

    info("*** Ready. Use CLI to test.\n")
    CLI(net)

    info("*** Stopping network\n")
    net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    simple_topology()
