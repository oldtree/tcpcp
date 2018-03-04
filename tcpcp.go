package tcpcp

import (
	"errors"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"time"
)

type Pool interface {
	Init() error
	Acquire() *BackendPoint
	Release(*BackendPoint)
	Clear()
}

type Connection struct {
	net.Conn
	Status int
}

func (c *Connection) Write(data []byte) (int, error) {
	length, err := c.Conn.Write(data)
	if err != nil {
		if c.Status != -1 {
			c.Status = -1
		}
		return 0, err
	}
	return length, nil
}

func (c *Connection) Read(data []byte) (int, error) {
	length, err := c.Conn.Read(data)
	if err != nil {
		if c.Status != -1 {
			c.Status = -1
		}
		return 0, err
	}
	return length, nil
}

func (c *Connection) Close() error {
	err := c.Close()
	if err != nil {
		if c.Status != -1 {
			c.Status = -1
		}
		return err
	}
	return nil
}

func (c *Connection) LocalAddr() net.Addr {
	return c.LocalAddr()
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.RemoteAddr()
}

func (c *Connection) SetDeadline(t time.Time) error {
	err := c.SetDeadline(t)
	if err != nil {
		if c.Status != -1 {
			c.Status = -1
		}
		return err
	}
	return nil
}

func (c *Connection) SetReadDeadline(t time.Time) error {
	err := c.SetReadDeadline(t)
	if err != nil {
		if c.Status != -1 {
			c.Status = -1
		}
		return err
	}
	return nil
}

func (c *Connection) SetWriteDeadline(t time.Time) error {
	err := c.SetWriteDeadline(t)
	if err != nil {
		if c.Status != -1 {
			c.Status = -1
		}
		return err
	}
	return nil
}

type BackendPoint struct {
	*Connection
}

type FrontendPoint struct {
	*Connection
}

type Bridge struct {
	Id        int64
	StartTime int64

	InterConn        *BackendPoint
	InterWriteLength int64
	InterReadLength  int64
	OuterConn        *FrontendPoint
	OuterWriteLength int64
	OuterReadLength  int64

	Stopchan  chan struct{}
	BreakChan chan struct{}

	p Pool

	IsClose bool
	BufSize int
	sync.Mutex
}

func NewBridge(id int64, out net.Conn, bufsize int, pool Pool, stopC chan struct{}) *Bridge {
	return &Bridge{
		Id:               id,
		StartTime:        time.Now().Unix(),
		OuterConn:        &FrontendPoint{&Connection{out, 1}},
		InterWriteLength: 0,
		InterReadLength:  0,
		InterConn:        pool.Acquire(),
		p:                pool,
		OuterWriteLength: 0,
		OuterReadLength:  0,
		Stopchan:         stopC,
		BreakChan:        make(chan struct{}, 2),
		IsClose:          false,
		BufSize:          bufsize,
	}
}

func (b *Bridge) Patrol() {
	tic := time.NewTicker(time.Second * 60)
	defer tic.Stop()
	for {
		select {
		case <-b.Stopchan:
			b.Stop()
			break
		case <-b.BreakChan:
			b.BreakArrow()
			break
		case <-tic.C:
			if b.InterConn.Status == -1 {
				b.BreakChan <- struct{}{}
			} else if b.OuterConn.Status == -1 {
				b.BreakChan <- struct{}{}
			} else {
				log.Printf("Bridge [%d] is well \n", b.Id)
			}
		}
	}
}

func (b *Bridge) Start() *Bridge {
	go b.Patrol()
	go func() {
		buf := make([]byte, b.BufSize)
		length, err := io.CopyBuffer(b.InterConn, b.OuterConn, buf)
		if err != nil {
			log.Println("copy to inter connection error : ", err)
			return
		}
		b.InterReadLength = length
		b.OuterWriteLength = length
	}()
	go func() {
		buf := make([]byte, b.BufSize)
		length, err := io.CopyBuffer(b.OuterConn, b.InterConn, buf)
		if err != nil {
			log.Println("copy to outer connetion error : ", err)
			return
		}
		b.InterWriteLength = length
		b.OuterReadLength = length
	}()
	return b
}

func (b *Bridge) BreakArrow() {
	b.Lock()
	defer b.Unlock()
	if b.IsClose == true {
		return
	} else {
		b.IsClose = true
	}
	b.OuterConn.Status = -1
	b.OuterConn.Close()
	b.p.Release(b.InterConn)
	return
}

func (b *Bridge) Stop() *Bridge {
	b.Lock()
	defer b.Unlock()
	if b.IsClose == true {
		return b
	} else {
		b.IsClose = true
	}
	b.OuterConn.Status = -1
	b.OuterConn.Close()
	b.InterConn.Status = -1
	b.InterConn.Close()
	return b
}

type BackEndPool struct {
	BackendAddress string
	Number         int
	backendMap     map[int64]*BackendPoint
	pool           chan *BackendPoint
}

func NewBackendPool(number int, address string) *BackEndPool {
	return &BackEndPool{
		BackendAddress: address,
		Number:         number,
		backendMap:     make(map[int64]*BackendPoint, number),
		pool:           make(chan *BackendPoint, number),
	}
}

func (b *BackEndPool) Init() error {
	for index := 0; index < b.Number; index++ {
		newconn, err := net.Dial("tcp", b.BackendAddress)
		if err != nil {
			log.Println("init backend pool failed : ", err)
			return err
		}
		cptr := &BackendPoint{&Connection{Status: 1, Conn: newconn}}
		b.backendMap[time.Now().Unix()] = cptr
		b.pool <- cptr
		time.Sleep(time.Second * 1)
	}
	return nil
}

func (b *BackEndPool) Acquire() *BackendPoint {
	return <-b.pool
}
func (b *BackEndPool) Release(back *BackendPoint) {
	b.pool <- back
	return
}

func (b *BackEndPool) Clear() {
	log.Println("start clear backend connection pool")
	for key, value := range b.backendMap {
		log.Printf("stop back connection [%d] \n", key)
		value.Connection.Close()
	}
	log.Println("end clear backend connetcion pool")
}

//TODO : check pool connetcion,if status == -1,rebuild this connection and re-put in pool
// func (b *BackEndPool)Patrol(){

// }

type Server struct {
	Address  string
	Protocol string
	Handler  HandleConn
}

func NewServer(address, protocol string, h HandleConn) *Server {
	return &Server{
		Address:  address,
		Protocol: protocol,
		Handler:  h,
	}
}

func (s *Server) Server() error {
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		log.Println("start server failed : ", err)
		return err
	}
	if listener == nil {
		return errors.New("build listener failed")
	}
	defer listener.Close()
	var retryCount int
	for {
	START:
		newcon, err := listener.Accept()
		if err != nil {
			if err.(net.Error).Temporary() == true {
				if retryCount > 5 {
					break
				}
				time.Sleep(time.Second * time.Duration(2^retryCount))
				retryCount++
				goto START
			}
		}
		go s.Handler(newcon)
	}
	return nil
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

}

var StopChan = make(chan struct{}, 1)
var pool = NewBackendPool(20, "127.0.0.1:8888")

type HandleConn func(conn net.Conn) error

func SimpleHandle(conn net.Conn) error {
	b := NewBridge(int64(time.Now().Nanosecond()), conn, 1024, pool, StopChan)
	b.StartTime = time.Now().Unix()
	b.Start()
	return nil
}

func Init() {
	defer func() {
		StopChan <- struct{}{}
	}()
	s := NewServer("127.0.0.1:80", "tcp", SimpleHandle)
	s.Server()
}
