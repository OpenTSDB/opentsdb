#!/usr/bin/python
#coding:utf-8

import atexit
import errno
import fcntl
import logging
import os
import random
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from logging.handlers import RotatingFileHandler
from Queue import Queue
from Queue import Empty
from Queue import Full
from optparse import OptionParser

# global variables.
COLLECTORS = {}
GENERATION = 0
DEFAULT_LOG = '/var/log/tcollector.log'
LOG = logging.getLogger('tcollector')
ALIVE = True

MAX_UNCAUGHT_EXCEPTIONS = 100

def register_collector(collector):
    """
    把对象collector放到全局字典COLLECTORS中，键为collector.name，值为collector，留日后使用保存的这些对象
    :param collector:
    :return:
    """
    #检查collector实例是否是Collector类的对象，如果不是，抛出异常错误
    assert isinstance(collector, Collector), "collector=%r" % (collector,)

    #如果对象已经在全局字典中存在，通过col.proc属性查看这个对象属否已经用来收集数据，如果已经使用，把它shutdown掉
    if collector.name in COLLECTORS:
        col = COLLECTORS[collector.name]
        if col.proc is not None:
            LOG.error('%s still has a process (pid=%d) and is being reset,terminating', col.name, col.proc.pid)
            col.shutdown()
    #为全局字典赋值
    COLLECTORS[collector.name] = collector

class ReaderQueue(Queue):
    """
    这个类继承自Queue，可以把数据通过该类的nput方法将数据放入队列中
    """
    def nput(self, value):
        """
        实现非堵塞的put，目的是如果队列已经满了不要等待直接抛出Queue.Full异常，否则put一直会等到队列空出位置为止
        :param value:放入队列的数据
        :return:如果插入队列成功返回True，否则False
        """
        try:
            #False实现非堵塞
            self.put(value, False)
        except Full:
            LOG.error("DROPPED LINE: %s", value)
            return False
        return True

class Collector(object):
    """
    收集器类，负责管理进程和从进程中获取数据
    """

    def __init__(self, colname, interval, filename, mtime=0, lastspawn=0):
        """Construct a new Collector."""
        self.name = colname
        self.interval = interval
        self.filename = filename
        self.lastspawn = lastspawn
        #self.proc会在使用的时候保存一个subprocess.Popen对象
        self.proc = None
        self.nextkill = 0
        self.killstate = 0
        self.dead = False
        self.mtime = mtime
        self.generation = GENERATION
        #来用保存临时数据的
        self.buffer = ""
        #从self.buffer中以"\n"分隔数据放到自己的囊中
        self.datalines = []
        self.values = {}
        self.lines_sent = 0
        self.lines_received = 0
        self.lines_invalid = 0

    def read(self):
        """
        读取subprocess.Popen对象执行的时候打印的结果：对于错误的结果，打印到日志，不做处理；只收集标准输出数据
        :return:
        """

        #读取self.proc这个进程的执行时候产生的错误输出，如果有，打印到日志中
        try:
            #读取self.proc进程的错误输出
            out = self.proc.stderr.read()
            #如果有错误输出，打印到日志，没干别的了
            if out:
                LOG.debug('reading %s got %d bytes on stderr',self.name, len(out))
                for line in out.splitlines():
                    LOG.warning('%s: %s', self.name, line)
        except IOError, (err, msg):
            if err != errno.EAGAIN:
                raise
        except:
            LOG.exception('uncaught exception in stderr read')

        #获取self.proc进程的标准输出数据，追加到self.buffer变量中
        try:
            self.buffer += self.proc.stdout.read()
            if len(self.buffer):
                LOG.debug('reading %s, buffer now %d bytes',self.name, len(self.buffer))
        except IOError, (err, msg):
            if err != errno.EAGAIN:
                raise
        except:
            LOG.exception('uncaught exception in stdout read')
            return

        #把self.buffer拆分为行，把这些行数据放入到self.datalines这个list变量中保存
        while self.buffer:
            idx = self.buffer.find('\n')
            #如果没有换行符，退出循环
            if idx == -1:
                break

            #一行一行的获取，代码看起来不pythonic
            line = self.buffer[0:idx].strip()
            if line:
                self.datalines.append(line)
            #每循环一次修改self.buffer变量
            self.buffer = self.buffer[idx+1:]

    def collect(self):
        """
        把read方法放入到self.datalines中的数据读取出来，yield给调用者，即调用者可以通过for来遍历方法collect的结果
        :return:
        """

        while self.proc is not None:
            self.read()
            if not len(self.datalines):
                return
            while len(self.datalines):
                yield self.datalines.pop(0)

    def shutdown(self):
        """
        kill掉self.proc进程
        :return:
        """
        if not self.proc:
            return
        try:
            #self.proc.poll执行的时候如果self.proc进程没有完成，那么poll就返回None，否则返回进程的exit code
            if self.proc.poll() is None:
                #进程没有完成，直接kill掉，默认发的是signal.SIGTERM信号给进程
                kill(self.proc)
                for attempt in range(5):
                    if self.proc.poll() is not None:
                        return
                    LOG.info('Waiting %ds for PID %d to exit...' % (5 - attempt, self.proc.pid))
                    time.sleep(1)
                #5次绅士般的kill不成功，那么不得不来点硬的了，发送signal.SIGKILL信号，由操作系统直接关闭，啥都不解释
                kill(self.proc, signal.SIGKILL)
                self.proc.wait()
        except:
            LOG.exception('ignoring uncaught exception while shutting down')

    def evict_old_keys(self, cut_off):
        """
        把时间戳小cut_off的数据删除掉
        :param cut_off: 时间戳
        :return:
        """
        for key in self.values.keys():
            time = self.values[key][3]
            if time < cut_off:
                del self.values[key]

class StdinCollector(Collector):
    """
    从标准输入获取数据的收集器，比较简单，一些self.proc就不需要指定到摸个进程了
    """

    def __init__(self):
        #调用父类的__init__方法注意传递的这三个变量
        super(StdinCollector, self).__init__('stdin', 0, '<stdin>')
        #因为从标准输出获取数据，所以self.proc用不到，直接给他个True，给其他也可以，这个不重要的
        self.proc = True

    def read(self):
        """
        从标准输出获取数据，sys.stdin.readline会block直到有数据进来
        :return:
        """

        global ALIVE
        line = sys.stdin.readline()
        if line:
            self.datalines.append(line.rstrip())
        else:
            ALIVE = False

    def shutdown(self):
        #因为self.proc没有对应的进程了，所以这个方法没有意义了，直接覆盖掉父类的方法
        pass

class ReaderThread(threading.Thread):
    def __init__(self, dedupinterval, evictinterval):
        """
        线程，用来从collector中获取数据，然后放到ReaderQueue中
        :param dedupinterval:设置多少秒内的相同数据不发送
        :param evictinterval:设置多少秒以前而且没有发送的数据，把他删除
        :return:
        """
        #保证evictinterval必须大于dedupinterval，否则不发送的数据很快被删除
        assert evictinterval > dedupinterval, "%r <= %r" % (evictinterval,dedupinterval)

        #调用threading.Thread的初始化函数__init_
        super(ReaderThread, self).__init__()

        #建立可以存放10k个数据的队列
        self.readerq = ReaderQueue(100000)
        self.lines_collected = 0
        self.lines_dropped = 0
        self.dedupinterval = dedupinterval
        self.evictinterval = evictinterval

    def run(self):

        LOG.debug("ReaderThread up and running")
        lastevict_time = 0

        while ALIVE:
            for col in all_living_collectors():
                for line in col.collect():
                    #处理从collector中获取到的数据，并传入self.readerq队列中存储
                    self.process_line(col, line)

            #删除一定时间之前的数据，那些数据当时发送不了了，现在发送也没有用了，暂时这么理解
            now = int(time.time())
            if now - lastevict_time > self.evictinterval:
                lastevict_time = now
                now -= self.evictinterval
                for col in all_collectors():
                    col.evict_old_keys(now)

            #循环每次到这里暂停1s
            time.sleep(1)

    def process_line(self, col, line):
        """
        处理从collector中获取到的数据
        :param col:
        :param line:
        :return:
        """
        #获取到数据以后修改col对象中的lines_received加1，标识此对象又发送了一行数据
        col.lines_received += 1
        #数据长度限制，这个是由于opentsdb.tsd.PipelineFactory限制了
        if len(line) >= 1024:
            LOG.warning('%s line too long: %s', col.name, line)
            #修改此对象中的lines_invalid，标识对象的行数据又失效了一行了
            col.lines_invalid += 1
            return
        #行数据的格式必须是：<监控指标名称> <时间戳> <数据量> [标签1,标签2...]
        #其中标签是任何的k,v对，如a=b等
        #例如：mccq.cpu.average 1361006731 15 host=mc_s1_192.168.1.1 project=m1
        parsed = re.match('^([-_./a-zA-Z0-9]+)\s+' # Metric name.
                          '(\d+)\s+'               # Timestamp.
                          '(\S+?)'                 # Value (int or float).
                          '((?:\s+[-_./a-zA-Z0-9]+=[-_./a-zA-Z0-9]+)*)$', # Tags
                          line)
        #如果不匹配数据，则打印错误日志，并修改col对象的lines_invalid加1
        if parsed is None:
            LOG.warning('%s sent invalid data: %s', col.name, line)
            col.lines_invalid += 1
            return
        metric, timestamp, value, tags = parsed.groups()
        timestamp = int(timestamp)

        key = (metric, tags)
        if key in col.values:
            #如果timestamp不大于前一个数据，忽略掉这行数据
            if timestamp <= col.values[key][3]:
                LOG.error("Timestamp out of order: metric=%s%s,"
                          " old_ts=%d >= new_ts=%d - ignoring data point"
                          " (value=%r, collector=%s)", metric, tags,
                          col.values[key][3], timestamp, value, col.name)
                col.lines_invalid += 1
                return

            #如果value与前一样相同，而timestamp与前一行的数据的timestamp差小于self.dedupinterval，不发送此数据，标识为True就行
            if (col.values[key][0] == value and
                (timestamp - col.values[key][3] < self.dedupinterval)):
                col.values[key] = (value, True, line, col.values[key][3])
                return

            #为了优化opentsdb的图形连续性，我们要把之前因为在规定的时间间隔内value相同的而保留下来的数据先发送了
            if ((col.values[key][1] or (timestamp - col.values[key][3] >= self.dedupinterval)) and col.values[key][0] != value):
                #col对象发送的数据量+1
                col.lines_sent += 1
                if not self.readerq.nput(col.values[key][2]):
                    #发送了数据，但是不成功，self.lines_dropped变量值+1
                    self.lines_dropped += 1
                else:
                    col.values[key][1] = False

        #将发送的数据放入到col对象的values字典中，同时发送这行数据
        col.values[key] = (value, False, line, timestamp)
        col.lines_sent += 1
        if not self.readerq.nput(line):
            self.lines_dropped += 1

class SenderThread(threading.Thread):
    def __init__(self, reader, dryrun, host, port, self_report_stats, tags):
        """
        将数据发送给tsd，该工具没有对失败的数据进行持久化，这样程序崩溃以后，已经获取但是还没有发送的数据就会丢失了
        :param reader:对ReaderTread实例的引用
        :param dryrun:是否是测试模式，如果是只会将数据打印到终端，而不发送给tsd数据库
        :param host:tsd地址
        :param port:tsd端口
        :param self_report_stats:是否也把tcollector自己的数据的统计发给tsd
        :param tags:额外的标签
        :return:
        """
        super(SenderThread, self).__init__()

        self.dryrun = dryrun
        self.host = host
        self.port = port
        self.reader = reader
        self.tagstr = tags
        self.tsd = None
        self.last_verify = 0
        self.sendq = []
        self.self_report_stats = self_report_stats

    def run(self):
        """
        循环发送数据给tsd，第一次获取，超时时间为5s，如果得到数据就再等5s，尝试再拿一行数据，然后把得到的数据全部发送出去
        """
        errors = 0
        while ALIVE:
            try:
                #检查到tsd的连接是否已经起来
                self.maintain_conn()
                try:
                    #从reader的队列中获取监控数据，堵塞，超时5s,如果队列为空，抛出Queue.Empty异常
                    line = self.reader.readerq.get(True, 5)
                except Empty:
                    continue
                self.sendq.append(line)
                #暂停5s
                time.sleep(5)
                while True:
                    try:
                        #再从reader中获取监控数据，非堵塞，如果没有数据直接抛出异常
                        line = self.reader.readerq.get(False)
                    except Empty:
                        break
                    self.sendq.append(line)

                #发送self.sendq中保存的数据
                self.send_data()
                errors = 0
            except (ArithmeticError, EOFError, EnvironmentError, LookupError,
                    ValueError), e:
                errors += 1
                #异常发生次数超过MAX_UNCAUGHT_EXCEPTIONS，就退出
                if errors > MAX_UNCAUGHT_EXCEPTIONS:
                    shutdown()
                    raise
                LOG.exception('Uncaught exception in SenderThread, ignoring')
                time.sleep(1)
                continue
            except:
                LOG.exception('Uncaught exception in SenderThread, going to exit')
                #遇到未知错误也退出
                shutdown()
                raise

    def verify_conn(self):
        """
        定期检查到tsd的连接，同时检查tsb是否正常工作
        """
        if self.tsd is None:
            return False

        #上次检查离现在少于60s的不在检查连接状况，直接返回True
        if self.last_verify > time.time() - 60:
            return True

        LOG.debug('verifying our TSD connection is alive')
        try:
            #通过发送"version\n"命令查看tsd连接状态
            self.tsd.sendall('version\n')
        except socket.error, msg:
            self.tsd = None
            return False

        bufsize = 4096
        while ALIVE:
            #尽可能多的读取tsb返回的数据，这个recv方法是会堵塞的，但是针对conn已经做了超时了
            try:
                buf = self.tsd.recv(bufsize)
            except socket.error, msg:
                self.tsd = None
                return False

            #如果没有获取到`version`命令返回的数据，说明tsd已经dead
            if not buf:
                self.tsd = None
                return False

            #获取一次还不过瘾，如果这次获取的数据长度是bufsize，continue，再获取，直到获取完为止，太贪婪了
            if len(buf) == bufsize:
                continue

            #如果要发送self_report_stats数据，先发送一些collector的搜集结果
            if self.self_report_stats:
                #先发送reader搜集的结果统计数据
                strs = [
                        ('reader.lines_collected',
                         '', self.reader.lines_collected),
                        ('reader.lines_dropped',
                         '', self.reader.lines_dropped)
                       ]

                #每个collector数据收集和发送的统计数据
                for col in all_living_collectors():
                    strs.append(('collector.lines_sent', 'collector='
                                 + col.name, col.lines_sent))
                    strs.append(('collector.lines_received', 'collector='
                                 + col.name, col.lines_received))
                    strs.append(('collector.lines_invalid', 'collector='
                                 + col.name, col.lines_invalid))

                ts = int(time.time())
                #每个元素的数据格式示例：tcollector.collector.lines_sent 1361029499 80 collector=system.io.aver
                strout = ["tcollector.%s %d %d %s" % (x[0], ts, x[2], x[1]) for x in strs]
                for string in strout:
                    self.sendq.append(string)
            #退出循环
            break

        #程序执行到这里，可以任务连接没有问题，更新self.last_verify的值
        self.last_verify = time.time()
        return True

    def maintain_conn(self):
        """
        管理collector到tsd的连接
        :return:
        """

        #如果是dryrun模式，不用继续检查
        if self.dryrun:
            return

        #检验连接状况，确保连接正常
        try_delay = 1
        while ALIVE:
            if self.verify_conn():
                return

            #延迟设计，最多延迟10分钟，延迟慢慢增加
            try_delay *= 1 + random.random()
            if try_delay > 600:
                try_delay *= 0.5
            LOG.debug('SenderThread blocking %0.2f seconds', try_delay)
            time.sleep(try_delay)

            #根据给定的参数host/port，相应的转换成一个包含用于创建socket对象的五元组，
            #该函数返回一个五元组，同时第五个参数sockaddr也是一个二元组(address, port)
            adresses = socket.getaddrinfo(self.host, self.port, socket.AF_UNSPEC,socket.SOCK_STREAM, 0)
            for family, socktype, proto, canonname, sockaddr in adresses:
                try:
                    #创建socket对象
                    self.tsd = socket.socket(family, socktype, proto)
                    #设置超时为15秒
                    self.tsd.settimeout(15)
                    #连接到tsd服务
                    self.tsd.connect(sockaddr)
                    #程序执行到这里，说明连接成功，退出循环
                    break
                except socket.error, msg:
                    LOG.warning('Connection attempt failed to %s:%d: %s',self.host, self.port, msg)
                self.tsd.close()
                self.tsd = None
            if not self.tsd:
                LOG.error('Failed to connect to %s:%d', self.host, self.port)

    def send_data(self):
        """
        将self.sendq中保存的数据发到tsb
        """

        #构造发送数据格式
        out = ''
        for line in self.sendq:
            line = 'put ' + line + self.tagstr
            out += line + '\n'
            LOG.debug('SENDING: %s', line)

        if not out:
            LOG.debug('send_data no data?')
            return

        #发送数据，这里需要完善，否则失败的话信息就丢失了
        try:
            if self.dryrun:
                print out
            else:
                self.tsd.sendall(out)
            self.sendq = []
        except socket.error, msg:
            LOG.error('failed to send data: %s', msg)
            try:
                self.tsd.close()
            except socket.error:
                pass
            self.tsd = None

def setup_logging(logfile=DEFAULT_LOG, max_bytes=None, backup_count=None):
    """
    设置日志参数
    :param logfile:保存日志的文件
    :param max_bytes: 日志文件最大值
    :param backup_count:备份日志的数量
    :return:
    """
    #设置日志级别为INFO
    LOG.setLevel(logging.INFO)
    if backup_count is not None and max_bytes is not None:
        assert backup_count > 0
        assert max_bytes > 0
        ch = RotatingFileHandler(logfile, 'a', max_bytes, backup_count)
    else:
        ch = logging.StreamHandler(sys.stdout)

    #日志格式
    ch.setFormatter(logging.Formatter('%(asctime)s %(name)s[%(process)d] '
                                      '%(levelname)s: %(message)s'))
    LOG.addHandler(ch)

def parse_cmdline(argv):
    """
    脚本执行选项设置
    :param argv:
    :return:
    """

    #获取默认程序路径
    default_cdir = os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])),
                                'collectors')
    #获取相关参数
    parser = OptionParser(description='Manages collectors which gather '
                                       'data and report back.')
    parser.add_option('-c', '--collector-dir', dest='cdir', metavar='DIR',
                      default=default_cdir,
                      help='Directory where the collectors are located.')
    parser.add_option('-d', '--dry-run', dest='dryrun', action='store_true',
                      default=False,
                      help='Don\'t actually send anything to the TSD, '
                           'just print the datapoints.')
    parser.add_option('-D', '--daemonize', dest='daemonize', action='store_true',
                      default=False, help='Run as a background daemon.')
    parser.add_option('-H', '--host', dest='host', default='localhost',
                      metavar='HOST',
                      help='Hostname to use to connect to the TSD.')
    parser.add_option('--no-tcollector-stats', dest='no_tcollector_stats',
                      default=False, action='store_true',
                      help='Prevent tcollector from reporting its own stats to TSD')
    parser.add_option('-s', '--stdin', dest='stdin', action='store_true',
                      default=False,
                      help='Run once, read and dedup data points from stdin.')
    parser.add_option('-p', '--port', dest='port', type='int',
                      default=4242, metavar='PORT',
                      help='Port to connect to the TSD instance on. '
                           'default=%default')
    parser.add_option('-v', dest='verbose', action='store_true', default=False,
                      help='Verbose mode (log debug messages).')
    parser.add_option('-t', '--tag', dest='tags', action='append',
                      default=[], metavar='TAG',
                      help='Tags to append to all timeseries we send, '
                           'e.g.: -t TAG=VALUE -t TAG2=VALUE')
    parser.add_option('-P', '--pidfile', dest='pidfile',
                      default='/var/run/tcollector.pid',
                      metavar='FILE', help='Write our pidfile')
    parser.add_option('--dedup-interval', dest='dedupinterval', type='int',
                      default=300, metavar='DEDUPINTERVAL',
                      help='Number of seconds in which successive duplicate '
                           'datapoints are suppressed before sending to the TSD. '
                           'default=%default')
    parser.add_option('--evict-interval', dest='evictinterval', type='int',
                      default=6000, metavar='EVICTINTERVAL',
                      help='Number of seconds after which to remove cached '
                           'values of old data points to save memory. '
                           'default=%default')
    parser.add_option('--max-bytes', dest='max_bytes', type='int',
                      default=64 * 1024 * 1024,
                      help='Maximum bytes per a logfile.')
    parser.add_option('--backup-count', dest='backup_count', type='int',
                      default=0, help='Maximum number of logfiles to backup.')
    parser.add_option('--logfile', dest='logfile', type='str',
                      default=DEFAULT_LOG,
                      help='Filename where logs are written to.')
    (options, args) = parser.parse_args(args=argv[1:])
    if options.dedupinterval < 2:
        parser.error('--dedup-interval must be at least 2 seconds')
    if options.evictinterval <= options.dedupinterval:
        parser.error('--evict-interval must be strictly greater than '
                     '--dedup-interval')

    if options.daemonize and not options.backup_count:
        options.backup_count = 1
    return (options, args)

def daemonize():
    """
    python产生守护进程的典型代码，两次fork，两次exit
    :return:
    """
    if os.fork():
        os._exit(0)
    os.chdir("/")
    os.umask(022)
    os.setsid()
    os.umask(0)
    if os.fork():
        os._exit(0)
    stdin = open(os.devnull)
    stdout = open(os.devnull, 'w')
    os.dup2(stdin.fileno(), 0)
    os.dup2(stdout.fileno(), 1)
    os.dup2(stdout.fileno(), 2)
    stdin.close()
    stdout.close()
    for fd in xrange(3, 1024):
        try:
            os.close(fd)
        except OSError:
            pass

def main(argv):
    """
    主程序开始
    :param argv:
    :return:
    """
    #分析参数和选项
    options, args = parse_cmdline(argv)

    #如果设置了守护进程选项，通过daemonize让程序进入守护进程模式
    if options.daemonize:
        daemonize()

    #设置日志相关参数
    setup_logging(options.logfile, options.max_bytes or None,
                  options.backup_count or None)

    #设置可见模式，即debug模式
    if options.verbose:
        LOG.setLevel(logging.DEBUG)

    #写pid文件
    if options.pidfile:
        write_pid(options.pidfile)

    #检查tags格式是否有效
    tags = {}
    for tag in options.tags:
        if re.match('^[-_.a-z0-9]+=\S+$', tag, re.IGNORECASE) is None:
            assert False, 'Tag string "%s" is invalid.' % tag
        k, v = tag.split('=', 1)
        if k in tags:
            assert False, 'Tag "%s" already declared.' % k
        tags[k] = v

    options.cdir = os.path.realpath(options.cdir)
    if not os.path.isdir(options.cdir):
        LOG.fatal('No such directory: %s', options.cdir)
        return 1
    #加载模块
    modules = load_etc_dir(options, tags)

    #设置host标签
    if not 'host' in tags and not options.stdin:
        tags['host'] = socket.gethostname()
        LOG.warning('Tag "host" not specified, defaulting to %s.', tags['host'])

    #构造tags字符串，以空格分开
    tagstr = ''
    if tags:
        tagstr = ' '.join('%s=%s' % (k, v) for k, v in tags.iteritems())
        tagstr = ' ' + tagstr.strip()

    #设置程序获取信号以后的动作，都是shutdown collector
    atexit.register(shutdown)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, shutdown_signal)

    #开始创建收集信息线程
    reader = ReaderThread(options.dedupinterval, options.evictinterval)
    #执行线程
    reader.start()

    #开始创建发送数据线程
    sender = SenderThread(reader, options.dryrun, options.host, options.port,
                          not options.no_tcollector_stats, tagstr)
    #执行线程
    sender.start()
    LOG.info('SenderThread startup complete')

    #收集和发送线程开始以后，开始执行具体的收集器了
    #如果从标准输入收集，把StdinCollector()实例通过register_collector注册到我们的收集器字典中
    if options.stdin:
        register_collector(StdinCollector())
        #stdin开始循环
        stdin_loop(options, modules, sender, tags)
    else:
        sys.stdin.close()
        #如果不是stdin模式，执行我们模块的收集方法
        main_loop(options, modules, sender, tags)

    LOG.debug('Shutting down -- joining the reader thread.')
    #等待reader退出
    reader.join()
    LOG.debug('Shutting down -- joining the sender thread.')
    #等待sender退出
    sender.join()

def stdin_loop(options, modules, sender, tags):
    """
    stdin模式下的循环函数，这里本质上就是让主线程堵塞用，以便读取stdin信息的线程正常获取信息，就没其他用途了
    :param options:
    :param modules:
    :param sender:
    :param tags:
    :return:
    """
    global ALIVE
    next_heartbeat = int(time.time() + 600)
    while ALIVE:
        time.sleep(15)
        reload_changed_config_modules(modules, options, sender, tags)
        now = int(time.time())
        if now >= next_heartbeat:
            LOG.info('Heartbeat (%d collectors running)'
                     % sum(1 for col in all_living_collectors()))
            next_heartbeat = now + 600

def main_loop(options, modules, sender, tags):
    """
    执行模块方法
    :param options:相关参数
    :param modules:模块列表
    :param sender:发送数据线程
    :param tags:自定义tags
    :return:
    """

    next_heartbeat = int(time.time() + 600)
    while True:
        #更新或者添加模块对应的collector
        populate_collectors(options.cdir)
        #重新加载模块
        reload_changed_config_modules(modules, options, sender, tags)
        reap_children()
        spawn_children()

        time.sleep(15)
        #检查collector的心跳，每10分钟一次
        now = int(time.time())
        if now >= next_heartbeat:
            LOG.info('Heartbeat (%d collectors running)'
                     % sum(1 for col in all_living_collectors()))
            next_heartbeat = now + 600

def list_config_modules(etcdir):
    """
    获取目录下的python文件列表
    :param etcdir:
    :return:
    """
    if not os.path.isdir(etcdir):
        return iter(())  # Empty iterator.
    return (name for name in os.listdir(etcdir)
            if (name.endswith('.py')
                and os.path.isfile(os.path.join(etcdir, name))))

def load_etc_dir(options, tags):
    """
    加载etc下的模块
    :param options:
    :param tags:
    :return:
    """

    etcdir = os.path.join(options.cdir, 'etc')
    #引入python lib 路径
    sys.path.append(etcdir)
    modules = {}
    #加载etc目录下的模块
    for name in list_config_modules(etcdir):
        path = os.path.join(etcdir, name)
        #加载模块
        module = load_config_module(name, options, tags)
        #将模块放入到modules字典中，注意字典的键值
        modules[path] = (module, os.path.getmtime(path))
    return modules

def load_config_module(name, options, tags):
    """
    加载模块
    :param name:模块的名称
    :param options:
    :param tags:
    :return:
    """

    if isinstance(name, str):
      LOG.info('Loading %s', name)
      d = {}
      # Strip the trailing .py
      module = __import__(name[:-3], d, d)
    else:
      module = reload(name)
    #如果模块有onload函数，执行这个函数，这个函数可以进行一些模块变量初始化
    onload = module.__dict__.get('onload')
    if callable(onload):
        try:
            onload(options, tags)
        except:
            LOG.fatal('Exception while loading %s', name)
            raise
    return module

def reload_changed_config_modules(modules, options, sender, tags):
    """
    重新加载变化了的模块
    :param modules:
    :param options:
    :param sender:
    :param tags:
    :return:
    """
    etcdir = os.path.join(options.cdir, 'etc')
    current_modules = set(list_config_modules(etcdir))
    current_paths = set(os.path.join(etcdir, name)
                        for name in current_modules)
    changed = False

    #加载时间戳变化了的模块
    for path, (module, timestamp) in modules.iteritems():
        if path not in current_paths:
            continue
        mtime = os.path.getmtime(path)
        if mtime > timestamp:
            LOG.info('Reloading %s, file has changed', path)
            module = load_config_module(module, options, tags)
            modules[path] = (module, mtime)
            changed = True

    #删除多余的模块
    for path in set(modules).difference(current_paths):
        LOG.info('%s has been removed, tcollector should be restarted', path)
        del modules[path]
        changed = True

    #检查是否有新的模块添加，如果有添加
    for name in current_modules:
        path = os.path.join(etcdir, name)
        if path not in modules:
            module = load_config_module(name, options, tags)
            modules[path] = (module, os.path.getmtime(path))
            changed = True

    if changed:
        sender.tagstr = ' '.join('%s=%s' % (k, v)
                                 for k, v in tags.iteritems())
        sender.tagstr = ' ' + sender.tagstr.strip()
    return changed

def write_pid(pidfile):
    """
    创建pid文件，把进程编号写入文件
    :param pidfile:
    :return:
    """
    f = open(pidfile, "w")
    try:
        f.write(str(os.getpid()))
    finally:
        f.close()

def all_collectors():
    """
    返回所有的collector
    :return:
    """
    return COLLECTORS.itervalues()

def all_valid_collectors():
    """
    返回有效的模块
    :return:
    """
    now = int(time.time())
    for col in all_collectors():
        #返回没有被标记dead和上次使用时间不得超过3600的collector
        if not col.dead or (now - col.lastspawn > 3600):
            yield col

def all_living_collectors():
    """
    返回所有正在执行的collector
    :return:
    """
    for col in all_collectors():
        if col.proc is not None:
            yield col

def shutdown_signal(signum, frame):
    """
    关闭掉整个进程
    :param signum:
    :param frame:
    :return:
    """
    LOG.warning("shutting down, got signal %d", signum)
    shutdown()

def kill(proc, signum=signal.SIGTERM):
    """
    发送signum信号给对应的进程
    :param proc:
    :param signum:
    :return:
    """
    os.kill(proc.pid, signum)

def shutdown():
    """
    关闭进程和线程
    :return:
    """
    global ALIVE
    #修改ALIVE全局变量
    if not ALIVE:
        return
    ALIVE = False

    LOG.info('shutting down children')

    #关闭掉collector
    for col in all_living_collectors():
        col.shutdown()

    LOG.info('exiting')
    sys.exit(1)

def reap_children():
    """
    检查收集数据模块执行子进程，看看是否有死的进程了，如果有，要不要重启它
    :return:
    """

    for col in all_living_collectors():
        now = int(time.time())
        status = col.proc.poll()
        if status is None:
            continue
        col.proc = None

        #跟进子进程退出状态决定是否需要重新注册collector
        if status == 13:
            LOG.info('removing %s from the list of collectors (by request)',
                      col.name)
            col.dead = True
        elif status != 0:
            LOG.warning('collector %s terminated after %d seconds with '
                        'status code %d, marking dead',
                        col.name, now - col.lastspawn, status)
            col.dead = True
        else:
            register_collector(Collector(col.name, col.interval, col.filename,
                                         col.mtime, col.lastspawn))

def set_nonblocking(fd):
    """
    设置fd为noblock模式
    :param fd:
    :return:
    """
    fl = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, fl)

def spawn_collector(col):
    """
    执行模块脚本
    :param col:
    :return:
    """

    LOG.info('%s (interval=%d) needs to be spawned', col.name, col.interval)

    try:
        col.proc = subprocess.Popen(col.filename, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE)
    except OSError, e:
        LOG.error('Failed to spawn collector %s: %s' % (col.filename, e))
        return
    col.lastspawn = int(time.time())
    set_nonblocking(col.proc.stdout.fileno())
    set_nonblocking(col.proc.stderr.fileno())
    if col.proc.pid > 0:
        col.dead = False
        LOG.info('spawned %s (pid=%d)', col.name, col.proc.pid)
        return
    LOG.error('failed to spawn collector: %s', col.filename)

def spawn_children():
    """
    执行子进程，进行收集数据
    :return:
    """

    for col in all_valid_collectors():
        now = int(time.time())
        if col.interval == 0:
            if col.proc is None:
                spawn_collector(col)
        elif col.interval <= now - col.lastspawn:
            if col.proc is None:
                spawn_collector(col)
                continue

            if col.nextkill > now:
                continue
            if col.killstate == 0:
                LOG.warning('warning: %s (interval=%d, pid=%d) overstayed '
                            'its welcome, SIGTERM sent',
                            col.name, col.interval, col.proc.pid)
                kill(col.proc)
                col.nextkill = now + 5
                col.killstate = 1
            elif col.killstate == 1:
                LOG.error('error: %s (interval=%d, pid=%d) still not dead, '
                           'SIGKILL sent',
                           col.name, col.interval, col.proc.pid)
                kill(col.proc, signal.SIGKILL)
                col.nextkill = now + 5
                col.killstate = 2
            else:
                LOG.error('error: %s (interval=%d, pid=%d) needs manual '
                           'intervention to kill it',
                           col.name, col.interval, col.proc.pid)
                col.nextkill = now + 300

def populate_collectors(coldir):
    """
    更新或者添加collector
    :param coldir:collector目录
    :return:
    """

    global GENERATION
    GENERATION += 1

    #获取以数字命名的目录列表
    for interval in os.listdir(coldir):
        if not interval.isdigit():
            continue
        interval = int(interval)

        #获取目录下的文件
        for colname in os.listdir('%s/%d' % (coldir, interval)):
            if colname.startswith('.'):
                continue

            filename = '%s/%d/%s' % (coldir, interval, colname)
            if os.path.isfile(filename) and os.access(filename, os.X_OK):
                #获取文件的修改时间
                mtime = os.path.getmtime(filename)

                if colname in COLLECTORS:
                    col = COLLECTORS[colname]
                    #相同colname的collector的interval不同，不允许
                    if col.interval != interval:
                        LOG.error('two collectors with the same name %s and '
                                   'different intervals %d and %d',
                                   colname, interval, col.interval)
                        continue

                    #根据修改时间，如果文件被修改了，重新注册collector
                    col.generation = GENERATION
                    if col.mtime < mtime:
                        LOG.info('%s has been updated on disk', col.name)
                        col.mtime = mtime
                        if not col.interval:
                            #如果是interval为0的文件，停掉正在运行的collector实例
                            col.shutdown()
                            LOG.info('Respawning %s', col.name)
                            register_collector(Collector(colname, interval,
                                                         filename, mtime))
                else:
                    #否则直接添加collector
                    register_collector(Collector(colname, interval, filename,
                                                 mtime))

    #删除掉那些已经不存在的模块
    to_delete = []
    for col in all_collectors():
        if col.generation < GENERATION:
            LOG.info('collector %s removed from the filesystem, forgetting',
                      col.name)
            col.shutdown()
            to_delete.append(col.name)
    for name in to_delete:
        del COLLECTORS[name]

if __name__ == '__main__':
    sys.exit(main(sys.argv))
