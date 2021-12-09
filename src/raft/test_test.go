package raft

//
// Raft tests.
//
// we will use the original test_test.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "testing"
import "fmt"
import "time"
import "math/rand"
import "sync/atomic"
import "sync"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
//使用心跳超时heartbeat timeout机制来触发 Leader 选举
const RaftElectionTimeout = 1000 * time.Millisecond

//2A Election测试的完成逻辑：
//1、开启3个服务器，检查能否选举出一个leader
//2、一个leader掉线，看能否选出一个新的leader
//3、旧的leader回归，不应该影响新的leader的状态（当然旧leader迅速被重新选举为leader也没问题）
//4、下线两个服务器，此时不应该有leader
//5、恢复一个机器，此时有两个机器。应该选举出一个leader
//6、恢复下线机器，三台服务器同时可用，此时原有leader的状态不应该被影响

func TestInitialElection2A(t *testing.T) {
	//调用Make启动servers个Raft，创建3个服务器，并互相连接，cfg.n指服务器的个数
	servers := 3
	//make_config，它创建N个raft节点的实例，并使他们互相连接。
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2A): initial election ...\n")

	// is a leader elected?检查是否有leader被选举出来
	cfg.checkOneLeader() ////checkOneLeader，这其实是测试代码的正确性检验的函数，也就是判断当前是否只有一个Leader，这里的GetState函数需要自己去实现，具体实现取决结构体如何设计

	// does the leader+term stay the same if there is no network failure?
	// term：Leader对应的任期
	term1 := cfg.checkTerms()           //查看当前term
	time.Sleep(2 * RaftElectionTimeout) //等待一段时间
	term2 := cfg.checkTerms()           //检查term是否发生改变（用于检测网络正常情况下是否有乱选举的情况）
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	fmt.Printf("  ... Passed\n")
}

func TestReElection2A(t *testing.T) {
	servers := 3
	//make_config，它创建N个raft节点的实例，并使他们互相连接。
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2A): election after network failure ...\n")
	//一个leader掉线，看能否选出一个新的leader
	leader1 := cfg.checkOneLeader()
	DPrintf("================ server %d disconnenct!!! ================\n", leader1)
	// if the leader disconnects, a new one should be elected.
	cfg.disconnect(leader1)
	cfg.checkOneLeader()

	//旧的leader回归，不应该影响新的leader的状态（当然旧leader迅速被重新选举为leader也没问题）
	DPrintf("================ server %d reconnenct!!! ================\n", leader1)
	cfg.connect(leader1)
	leader2 := cfg.checkOneLeader()
	//下线两个服务器，此时不应该有leader
	// if there's no quorum, no leader should
	// be elected.
	DPrintf("================ server %d && %d disconnenct!!! ================\n", leader2, (leader2+1)%servers)
	cfg.disconnect(leader2)
	cfg.disconnect((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)
	cfg.checkNoLeader()
	//恢复一个机器，此时有两个机器。应该选举出一个leader
	DPrintf("================ server %d reconnenct!!! ================\n", (leader2+1)%servers)
	cfg.connect((leader2 + 1) % servers)
	cfg.checkOneLeader()
	//恢复下线机器，三台服务器同时可用，此时原有leader的状态不应该被影响
	DPrintf("================ server %d reconnenct!!! ================\n", leader2)
	cfg.connect(leader2)
	cfg.checkOneLeader()

	fmt.Printf("  ... Passed\n")
}

// 2B BasicAgreement测试的完成逻辑
// 1、要添加一个新日志需要先找到leader，因为leader最先添加日志
// 2、所以第一个要完成的函数是raft.start()：如果该raft服务器不是leader，会返回false，继续找leader
// 如果是leader，start()函数把日志添加进leader的日志列表，同时附上日志的索引值和任期号。
// 3、接下来等待leader将日志同步给follower，如果大部分follower都确认该日志提交了，测试通过。
// 4、什么情况下同步会失败呢？那就是找到了错误的leader（先宕机后重启，状态还是leader没改成follower），
// 这个时候就等待heartBeat处理，在heartBeat中把状态改成follower，heartBeat进程在make函数中已经开始了
// 5、所以第二个要完成的函数是服务器对heartBeat的处理函数（本project中是AppendEntries函数）
// 另外同步日志的处理也是AppendEntries函数实现的
// 6、最后一个要更新的函数是startAppendEntries，补充leader接收到heartBeat（AppendEntries）反馈信息后的后续操作
// 状态更新后就能找到准确的leader了，测试通过。
func TestBasicAgree2B(t *testing.T) {
	// 创建五个server
	servers := 5
	cfg := make_config(t, servers, false)
	// 重置变量
	defer cfg.cleanup()

	fmt.Printf("Test (2B): basic agreement ...\n")

	// 测试逐步添加三个新日志
	iters := 3
	for index := 1; index < iters+1; index++ {
		// 有多少server认为序号为index的日志已经提交了
		nd, _ := cfg.nCommitted(index)
		// 事实上，序号为index的日志正打算添加，所以不应该有sever发现index已经提交了
		if nd > 0 {
			t.Fatalf("some have committed before Start()")
		}
		// 发送序号为index的指令给各个服务器，（leader最先收到，然后同步给follower）完成一次一致性检查
		xindex := cfg.one(index*100, servers)
		// 说明没达成一致性
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
	}

	fmt.Printf("  ... Passed\n")
}

// 2B FailAgreement测试的完成逻辑
// 在正常运行的分布式环境中完成日志添加和同步
// 断开一个follower完成日志添加和同步
// 等待一个选举周期完成日志添加和同步
// 恢复follower连接，能完成日志添加和同步
// 等待一个选举周期完成日志添加和同步
// 因为涉及到follower的断开和重启，所以要更新startAppendEntries函数：
// 1、在发送AppendEntries（heartBeat）之前开始重新选举了，就不是leader了，不能进行发送
// 2、在唯一的leader发送AppendEntries（heartBeat）时，如果有follower宕机了也没关系，继续向其他follower发送
func TestFailAgree2B(t *testing.T) {
	// 建立新的分布式环境，3个server
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): agreement despite follower disconnection ...\n")
	// 完成一次日志的添加与同步
	cfg.one(101, servers)

	// follower network disconnection
	// 找到leader
	leader := cfg.checkOneLeader()
	DPrintf("================ server %d disconnected!!! ================\n", (leader+1)%servers)
	// 断开除leader外的一个follower
	cfg.disconnect((leader + 1) % servers)

	// agree despite one disconnected server?
	// 依旧能完成日志的添加和同步
	cfg.one(102, servers-1)
	cfg.one(103, servers-1)
	// 等到下一个选举周期完成
	time.Sleep(RaftElectionTimeout)
	// 继续能完成日志的添加和同步
	cfg.one(104, servers-1)
	cfg.one(105, servers-1)
	// re-connect，重新将follower接通加入
	DPrintf("================ server %d reconnected!!! ================\n", (leader+1)%servers)
	cfg.connect((leader + 1) % servers)
	// agree with full set of servers?
	// 在所有server上能完成日志的添加和同步
	cfg.one(106, servers)
	// 等到下一个选举周期完成
	time.Sleep(RaftElectionTimeout)
	// 依旧能完成日志的添加和同步
	cfg.one(107, servers)

	fmt.Printf("  ... Passed\n")
}

// 2B FailNoAgree测试的完成逻辑
// 在正常运行的分布式环境中完成日志添加和同步
// 断开大多数follower，给leader添加一条新日志
// 等待2个选举周期，由于大多数follower宕机了，我们不能提交该日志，该日志不应该被现存的follower发现已经提交
// 恢复follower连接，再给leader添加一条新日志，注意由于宕机的server占大多数，所以新leader可能从宕机的服务器中选出
// 查看新日志是否被leader添加成功
// 实现时要补充的逻辑是：
// 1、leader在提交日志之前，要先检查大多数server是否收到了该日志
// 只有超半数的server都收到了该日志，leader才更新提交的日志的索引，并执行对应的日志
// 所以函数startAppendEntries需要修改，添加leader收到AppendEntries反馈后的检查
// 2、同样，follower接收到心跳包后的处理函数AppendEntries也需要进行修改
// 如果leader发过来的日志leader还没有提交，follower就不更新自己的最新提交的索引号，
// 等到下一个心跳包如果leader提交了，follower才更新提交的日志索引，执行对应的日志。
// 本project中执行就是发送包含命令的msg，config通过rpc收到server发过来的msg，记录统计每个server提交的日志项
func TestFailNoAgree2B(t *testing.T) {
	// 构建新的raft环境
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): no agreement if too many followers disconnect ...\n")
	// 正常情况下同步完成
	cfg.one(10, servers)

	// 3 of 5 followers disconnect
	leader := cfg.checkOneLeader()
	DPrintf("================ server %d && %d && %d disconnected!!! ================\n", (leader+1)%servers, (leader+2)%servers, (leader+3)%servers)
	cfg.disconnect((leader + 1) % servers)
	cfg.disconnect((leader + 2) % servers)
	cfg.disconnect((leader + 3) % servers)

	// leader添加一条日志
	index, _, ok := cfg.rafts[leader].Start(20)
	if ok != true {
		t.Fatalf("leader rejected Start()")
	}
	// 这是第二条指令
	if index != 2 {
		t.Fatalf("expected index 2, got %v", index)
	}
	// 等待2次新的选举周期
	time.Sleep(2 * RaftElectionTimeout)
	// 有多少server发现了第二条提交的指令
	n, _ := cfg.nCommitted(index)
	// 在大多数宕机的情况下是不能检测到第二条提交的指令的
	if n > 0 {
		t.Fatalf("%v committed but no majority", n)
	}
	DPrintf("================ server %d && %d && %d reconnected!!! ================\n", (leader+1)%servers, (leader+2)%servers, (leader+3)%servers)
	// repair重启这三台follower
	cfg.connect((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.connect((leader + 3) % servers)

	// the disconnected majority may have chosen a leader from
	// among their own ranks, forgetting index 2.
	// or perhaps
	// 可能新的leader是从三台宕机又恢复的server中选出的
	leader2 := cfg.checkOneLeader()
	index2, _, ok2 := cfg.rafts[leader2].Start(30)
	if ok2 == false {
		t.Fatalf("leader2 rejected Start()")
	}
	// 因此index2可以为2，也可以为3
	// 2代表新leader是从三台宕机又恢复的server中选出的，它们忽视了宕机时被执行的指令
	// 3代表新leader依旧是旧有的两台server中选出的
	if index2 < 2 || index2 > 3 {
		t.Fatalf("unexpected index %v", index2)
	}

	cfg.one(1000, servers)
	fmt.Printf("  ... Passed\n")
}

// 2B ConcurrentStarts测试的完成逻辑
// 本测试有五轮迭代，希望在某一次迭代中有如下情况能通过：
// 向leader启动start添加一条日志，仍是同一个leader再并发地启动五个start添加五条日志
// 某一次迭代中如果能有一次各个server同步成功就通过测试
// 实现时只要任期号不频繁变动就没有问题，涉及的是心跳包的间隔时间和重新选举的时间调整
// 比较work的时间设置是：选举超时: 200ms-400ms, 领导者心跳: 100ms
func TestConcurrentStarts2B(t *testing.T) {
	// 新的3个server的raft环境
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): concurrent Start()s ...\n")

	var success bool
loop:
	// 五轮迭代测试
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := cfg.checkOneLeader()
		// 向leader添加一条命令
		_, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		// 并发再启动5个start命令（并发再向leader添加五条索引）
		iters := 5
		var wg sync.WaitGroup
		is := make(chan int, iters)
		for ii := 0; ii < iters; ii++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				i, term1, ok := cfg.rafts[leader].Start(100 + i)
				// 希望还是同一个leader
				if term1 != term {
					return
				}
				if ok != true {
					return
				}
				is <- i
			}(ii)
		}

		wg.Wait()
		close(is)

		for j := 0; j < servers; j++ {
			// 希望leader没有改变
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		cmds := []int{}
		for index := range is {
			// 等待各个server提交日志
			cmd := cfg.wait(index, servers, term)
			if ix, ok := cmd.(int); ok {
				if ix == -1 {
					// peers have moved on to later terms
					// so we can't expect all Start()s to
					// have succeeded
					failed = true
					break
				}
				// 统计同步的日志
				cmds = append(cmds, ix)
			} else {
				t.Fatalf("value %v is not an int", cmd)
			}
		}

		if failed {
			// avoid leaking goroutines
			go func() {
				for range is {
				}
			}()
			continue
		}

		for ii := 0; ii < iters; ii++ {
			x := 100 + ii
			ok := false
			for j := 0; j < len(cmds); j++ {
				if cmds[j] == x {
					ok = true
				}
			}
			// 有cmd没有同步成功
			if ok == false {
				t.Fatalf("cmd %v missing in %v", x, cmds)
			}
		}

		success = true
		break
	}
	// 五轮迭代测试里，没有leader一直保持不变的情况
	// leader换得太频繁了
	if !success {
		t.Fatalf("term changed too often")
	}

	fmt.Printf("  ... Passed\n")
}

// 2B Rejoin测试的完成逻辑
// 将leader的宕机容错处理纳入考虑
// 1、三台服务器先同步一次
// 2、断开leader1，同步一次
// 3、在leader1上添加3个日志项，显然不能成功
// 4、等新leader选出了，完成一次日志的添加同步
// 5、又断开leader2，把leader1连回来，完成一次日志的添加同步
// 6、最后把leader2连回来，完成一次日志的添加同步
// 要修改的是startAppendEntries函数：
// 1、首先leader断开了再恢复就会变成follower
// 2、但是此时旧leader的状态还没改成follower，所以会继续发送心跳包（旧任期号倒是不影响其他follower，不会被接收）
// 3、但每一次发送心跳包之前还是都检查一下状态，看旧leader的状态有没有变成follower，那就不用再发送心跳包了。
func TestRejoin2B(t *testing.T) {
	// 3个server的raft环境
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): rejoin of partitioned leader ...\n")
	// 正常执行一次日志添加与同步
	cfg.one(101, servers)

	// leader network failure
	leader1 := cfg.checkOneLeader()
	DPrintf("================ server %d disconnected!!! ================\n", leader1)
	// 将当前leader，leader1断开
	cfg.disconnect(leader1)

	// 在leader1上添加3个日志项，显然不能成功
	// make old leader try to agree on some entries
	cfg.rafts[leader1].Start(102)
	cfg.rafts[leader1].Start(103)
	cfg.rafts[leader1].Start(104)

	// 等新leader选出了，完成一次日志的添加同步
	// new leader commits, also for index=2
	cfg.one(103, 2)

	// 又把leader2选出来
	// new leader network failure
	leader2 := cfg.checkOneLeader()
	DPrintf("================ server %d disconnected!!! ================\n", leader2)
	cfg.disconnect(leader2)

	// 又把leader1连回来
	// old leader connected again
	DPrintf("================ server %d reconnected!!! ================\n", leader1)
	cfg.connect(leader1)

	// 完成一次日志的添加同步
	cfg.one(104, 2)

	// 又把leader2加回来
	// all together now
	DPrintf("================ server %d reconnected!!! ================\n", leader2)
	cfg.connect(leader2)

	// 又完成一次日志的添加同步
	cfg.one(105, servers)

	fmt.Printf("  ... Passed\n")
}

func TestBackup2B(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): leader backs up quickly over incorrect follower logs ...\n")

	//cfg.one(rand.Int(), servers)
	cfg.one(10, servers)

	// put leader and one follower in a partition
	leader1 := cfg.checkOneLeader()
	DPrintf("================ server %d && %d && %d disconnected!!! ================\n", (leader1+2)%servers, (leader1+3)%servers, (leader1+4)%servers)
	cfg.disconnect((leader1 + 2) % servers)
	cfg.disconnect((leader1 + 3) % servers)
	cfg.disconnect((leader1 + 4) % servers)

	// submit lots of commands that won't commit
	for i := 0; i < 5; i++ {
		//cfg.rafts[leader1].Start(rand.Int())
		cfg.rafts[leader1].Start(i)
	}

	time.Sleep(RaftElectionTimeout / 2)
	DPrintf("================ server %d && %d disconnected!!! ================\n", (leader1+0)%servers, (leader1+1)%servers)
	cfg.disconnect((leader1 + 0) % servers)
	cfg.disconnect((leader1 + 1) % servers)

	// allow other partition to recover
	DPrintf("================ server %d && %d && %d reconnected!!! ================\n", (leader1+2)%servers, (leader1+3)%servers, (leader1+4)%servers)
	cfg.connect((leader1 + 2) % servers)
	cfg.connect((leader1 + 3) % servers)
	cfg.connect((leader1 + 4) % servers)

	// lots of successful commands to new group.
	for i := 0; i < 5; i++ {
		//cfg.one(rand.Int(), 3)
		cfg.one(i+50, 3)
	}

	// now another partitioned leader and one follower
	leader2 := cfg.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	DPrintf("================ server %d disconnected!!! ================\n", other)
	cfg.disconnect(other)

	// lots more commands that won't commit
	for i := 0; i < 5; i++ {
		//cfg.rafts[leader2].Start(rand.Int())
		cfg.rafts[leader2].Start(i + 100)
	}

	time.Sleep(RaftElectionTimeout / 2)

	// bring original leader back to life,
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
	}
	DPrintf("================ server %d && %d && %d reconnected!!! ================\n", (leader1+0)%servers, (leader1+1)%servers, other)
	cfg.connect((leader1 + 0) % servers)
	cfg.connect((leader1 + 1) % servers)
	cfg.connect(other)

	// lots of successful commands to new group.
	for i := 0; i < 5; i++ {
		//cfg.one(rand.Int(), 3)
		cfg.one(i+150, 3)
	}

	// now everyone
	for i := 0; i < servers; i++ {
		cfg.connect(i)
	}
	cfg.one(rand.Int(), servers)
	fmt.Printf("  ... Passed\n")
}

func TestCount2B(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2B): RPC counts aren't too high ...\n")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += cfg.rpcCount(j)
		}
		return
	}

	leader := cfg.checkOneLeader()

	total1 := rpcs()
	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool
loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = cfg.checkOneLeader()
		total1 = rpcs()
		iters := 10
		starti, term, ok := cfg.rafts[leader].Start(1)
		if !ok {
			// leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters+2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := cfg.rafts[leader].Start(x)
			if term1 != term {
				// Term changed while starting
				continue loop
			}
			if !ok {
				// No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := cfg.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i-1] {
				if ix == -1 {
					// term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti+i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := cfg.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += cfg.rpcCount(j)
		}

		if failed {
			continue loop
		}
		DPrintf("Total2: %d\n", total2)
		DPrintf("Total1: %d\n", total1)
		if total2-total1 > (iters+1+3)*3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2-total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += cfg.rpcCount(j)
	}
	DPrintf("Total3: %d\n", total3)
	if total3-total2 > 3*20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3-total2)
	}

	fmt.Printf("  ... Passed\n")
}

func TestPersist12C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): basic persistence ...\n")

	cfg.one(11, servers)

	// crash and re-start all
	for i := 0; i < servers; i++ {
		DPrintf("================ crash and restart %d!!! ================\n", i)
		cfg.start1(i)
	}
	for i := 0; i < servers; i++ {
		cfg.disconnect(i)
		cfg.connect(i)
	}

	cfg.one(12, servers)

	leader1 := cfg.checkOneLeader()
	cfg.disconnect(leader1)
	cfg.start1(leader1)
	cfg.connect(leader1)

	cfg.one(13, servers)

	leader2 := cfg.checkOneLeader()
	cfg.disconnect(leader2)
	cfg.one(14, servers-1)
	cfg.start1(leader2)
	cfg.connect(leader2)

	cfg.wait(4, servers, -1) // wait for leader2 to join before killing i3

	i3 := (cfg.checkOneLeader() + 1) % servers
	cfg.disconnect(i3)
	cfg.one(15, servers-1)
	cfg.start1(i3)
	cfg.connect(i3)

	cfg.one(16, servers)

	fmt.Printf("  ... Passed\n")
}

func TestPersist22C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): more persistence ...\n")

	index := 1
	for iters := 0; iters < 5; iters++ {
		cfg.one(10+index, servers)
		index++

		leader1 := cfg.checkOneLeader()

		cfg.disconnect((leader1 + 1) % servers)
		cfg.disconnect((leader1 + 2) % servers)

		cfg.one(10+index, servers-2)
		index++

		cfg.disconnect((leader1 + 0) % servers)
		cfg.disconnect((leader1 + 3) % servers)
		cfg.disconnect((leader1 + 4) % servers)

		cfg.start1((leader1 + 1) % servers)
		cfg.start1((leader1 + 2) % servers)
		cfg.connect((leader1 + 1) % servers)
		cfg.connect((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		cfg.start1((leader1 + 3) % servers)
		cfg.connect((leader1 + 3) % servers)

		cfg.one(10+index, servers-2)
		index++

		cfg.connect((leader1 + 4) % servers)
		cfg.connect((leader1 + 0) % servers)
	}

	cfg.one(1000, servers)

	fmt.Printf("  ... Passed\n")
}

func TestPersist32C(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): partitioned leader and one follower crash, leader restarts ...\n")

	cfg.one(101, 3)

	leader := cfg.checkOneLeader()
	cfg.disconnect((leader + 2) % servers)

	cfg.one(102, 2)

	cfg.crash1((leader + 0) % servers)
	cfg.crash1((leader + 1) % servers)
	cfg.connect((leader + 2) % servers)
	cfg.start1((leader + 0) % servers)
	cfg.connect((leader + 0) % servers)

	cfg.one(103, 2)

	cfg.start1((leader + 1) % servers)
	cfg.connect((leader + 1) % servers)

	cfg.one(104, servers)

	fmt.Printf("  ... Passed\n")
}

//
// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
//
func TestFigure82C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, false)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): Figure 8 ...\n")

	cfg.one(rand.Int(), 1)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if cfg.rafts[i] != nil {
				_, _, ok := cfg.rafts[i].Start(rand.Int())
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			cfg.crash1(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.rafts[s] == nil {
				cfg.start1(s)
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int(), servers)

	fmt.Printf("  ... Passed\n")
}

func TestUnreliableAgree2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): unreliable agreement ...\n")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				cfg.one((100*iters)+j, 1)
			}(iters, j)
		}
		cfg.one(iters, 1)
	}

	cfg.setunreliable(false)

	wg.Wait()

	cfg.one(100, servers)

	fmt.Printf("  ... Passed\n")
}

func TestFigure8Unreliable2C(t *testing.T) {
	servers := 5
	cfg := make_config(t, servers, true)
	defer cfg.cleanup()

	fmt.Printf("Test (2C): Figure 8 (unreliable) ...\n")

	cfg.one(rand.Int()%10000, 1)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			cfg.setlongreordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := cfg.rafts[i].Start(rand.Int() % 10000)
			if ok && cfg.connected[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := (rand.Int63() % 13)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			cfg.disconnect(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if cfg.connected[s] == false {
				cfg.connect(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if cfg.connected[i] == false {
			cfg.connect(i)
		}
	}

	cfg.one(rand.Int()%10000, servers)

	fmt.Printf("  ... Passed\n")
}

func internalChurn(t *testing.T, unreliable bool) {

	if unreliable {
		fmt.Printf("Test (2C): unreliable churn ...\n")
	} else {
		fmt.Printf("Test (2C): churn ...\n")
	}

	servers := 5
	cfg := make_config(t, servers, unreliable)
	defer cfg.cleanup()

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				cfg.mu.Lock()
				rf := cfg.rafts[i]
				cfg.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := cfg.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							cfg.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			cfg.disconnect(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if cfg.rafts[i] == nil {
				cfg.start1(i)
			}
			cfg.connect(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if cfg.rafts[i] != nil {
				cfg.crash1(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	cfg.setunreliable(false)
	for i := 0; i < servers; i++ {
		if cfg.rafts[i] == nil {
			cfg.start1(i)
		}
		cfg.connect(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := cfg.one(rand.Int(), servers)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := cfg.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			cfg.t.Fatalf("didn't find a value")
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestReliableChurn2C(t *testing.T) {
	internalChurn(t, false)
}

func TestUnreliableChurn2C(t *testing.T) {
	internalChurn(t, true)
}
