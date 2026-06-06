package vless

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	vmess "github.com/Overwhelmed44/dynamic-vless"
	"github.com/sagernet/sing/common/auth"
	"github.com/sagernet/sing/common/buf"
	"github.com/sagernet/sing/common/bufio"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"

	"github.com/gofrs/uuid/v5"
)

type Service[T comparable] struct {
	userMap    map[[16]byte]T
	userFlow   map[T]string
	logger     logger.Logger
	handler    Handler
	cache      *ProfileCache
	fetchURL   string
	apiSecret  []byte
	masterUUID string
}

type Handler interface {
	N.TCPConnectionHandlerEx
	N.UDPConnectionHandlerEx
}

func NewService[T comparable](logger logger.Logger, handler Handler) *Service[T] {
	cache, err := strconv.Atoi(os.Getenv("CACHE_SIZE"))
	if err != nil || cache == 0 {
		cache = 1000
	}
	fetchURL := os.Getenv("FETCH_URL")
	apiSecret, err := hex.DecodeString(os.Getenv("API_SECRET_KEY"))
	if err != nil {
		apiSecret = []byte("")
	}
	masterUUID := strings.ReplaceAll(os.Getenv("MASTER_UUID"), "-", "")

	return &Service[T]{
		logger:     logger,
		handler:    handler,
		cache:      NewProfileCache(cache),
		fetchURL:   fetchURL,
		apiSecret:  apiSecret,
		masterUUID: masterUUID,
	}
}

func (s *Service[T]) UpdateUsers(userList []T, userUUIDList []string, userFlowList []string) {
	userMap := make(map[[16]byte]T)
	userFlowMap := make(map[T]string)
	for i, userName := range userList {
		userID, err := uuid.FromString(userUUIDList[i])
		if err != nil {
			userID = uuid.NewV5(uuid.Nil, userUUIDList[i])
		}
		userMap[userID] = userName
		userFlowMap[userName] = userFlowList[i]
	}
	s.userMap = userMap
	s.userFlow = userFlowMap
}

func (s *Service[T]) NewConnection(ctx context.Context, conn net.Conn, source M.Socksaddr, onClose N.CloseHandlerFunc) error {
	request, err := ReadRequest(conn)
	if err != nil {
		return err
	}

	// fetching profile

	profileUUID := uuid.FromBytesOrNil(request.UUID[:])
	if profileUUID.IsNil() {
		return E.New()
	}

	asString := profileUUID.String()
	if (strings.ReplaceAll(asString, "-", "") != s.masterUUID) && (!s.cache.IsCached(asString)) {
		rctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		body := map[string]interface{}{
			"uuid": asString,
			"ip":   source.AddrString(),
		}
		data, err := json.Marshal(body)

		if err != nil {
			return E.New("JSON error")
		}

		req, err := http.NewRequestWithContext(rctx, "POST", s.fetchURL, bytes.NewBuffer(data))

		if err != nil {
			return E.New("Request error: ", err)
		}

		req.Header.Set("Content-Type", "application/json")
		token, err := CreateAccessToken(s.apiSecret)
		if err != nil {
			return E.New("Token creation error: ", err)
		}
		req.Header.Set("Authorization", token)

		client := &http.Client{}
		resp, err := client.Do(req)

		if err == nil {
			resp.Body.Close()

			if resp.StatusCode == 401 {
				return E.New("UUID ", profileUUID, " is not allowed")
			}
		}
	}
	s.cache.Pair(asString, source.Addr)

	user := asString

	ctx = auth.ContextWithUser(ctx, user)
	userFlow := request.Flow

	if request.Flow == FlowVision && request.Command == vmess.NetworkUDP {
		return E.New(FlowVision, " flow does not support UDP")
	} else if request.Flow != userFlow {
		return E.New("flow mismatch: expected ", flowName(userFlow), ", but got ", flowName(request.Flow))
	}

	if request.Command == vmess.CommandUDP {
		s.handler.NewPacketConnectionEx(ctx, &serverPacketConn{ExtendedConn: bufio.NewExtendedConn(conn), destination: request.Destination}, source, request.Destination, onClose)
		return nil
	}
	responseConn := &serverConn{ExtendedConn: bufio.NewExtendedConn(conn)}
	switch userFlow {
	case FlowVision:
		conn, err = NewVisionConn(responseConn, conn, request.UUID, s.logger)
		if err != nil {
			return E.Cause(err, "initialize vision")
		}
	case "":
		conn = responseConn
	default:
		return E.New("unknown flow: ", userFlow)
	}
	switch request.Command {
	case vmess.CommandTCP:
		s.handler.NewConnectionEx(ctx, conn, source, request.Destination, onClose)
		return nil
	case vmess.CommandMux:
		return vmess.HandleMuxConnection(ctx, conn, source, s.handler)
	default:
		return E.New("unknown command: ", request.Command)
	}
}

func flowName(value string) string {
	if value == "" {
		return "none"
	}
	return value
}

type serverConn struct {
	N.ExtendedConn
	responseWritten bool
}

func (c *serverConn) Read(b []byte) (n int, err error) {
	return c.ExtendedConn.Read(b)
}

func (c *serverConn) Write(b []byte) (n int, err error) {
	if !c.responseWritten {
		buffer := buf.NewSize(2 + len(b))
		buffer.WriteByte(Version)
		buffer.WriteByte(0)
		buffer.Write(b)
		_, err = c.ExtendedConn.Write(buffer.Bytes())
		buffer.Release()
		if err == nil {
			n = len(b)
		}
		c.responseWritten = true
		return
	}
	return c.ExtendedConn.Write(b)
}

func (c *serverConn) WriteBuffer(buffer *buf.Buffer) error {
	if !c.responseWritten {
		header := buffer.ExtendHeader(2)
		header[0] = Version
		header[1] = 0
		c.responseWritten = true
	}
	return c.ExtendedConn.WriteBuffer(buffer)
}

func (c *serverConn) FrontHeadroom() int {
	if c.responseWritten {
		return 0
	}
	return 2
}

func (c *serverConn) ReaderReplaceable() bool {
	return true
}

func (c *serverConn) WriterReplaceable() bool {
	return c.responseWritten
}

func (c *serverConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *serverConn) Upstream() any {
	return c.ExtendedConn
}

type serverPacketConn struct {
	N.ExtendedConn
	responseWritten bool
	destination     M.Socksaddr
}

func (c *serverPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	var packetLen uint16
	err = binary.Read(c.ExtendedConn, binary.BigEndian, &packetLen)
	if err != nil {
		return
	}
	if len(p) < int(packetLen) {
		err = io.ErrShortBuffer
		return
	}
	n, err = io.ReadFull(c.ExtendedConn, p[:packetLen])
	if err != nil {
		return
	}
	if c.destination.IsFqdn() {
		addr = c.destination
	} else {
		addr = c.destination.UDPAddr()
	}
	return
}

func (c *serverPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	if !c.responseWritten {
		_, err = c.ExtendedConn.Write([]byte{Version, 0})
		if err != nil {
			return
		}
		c.responseWritten = true
	}
	err = binary.Write(c.ExtendedConn, binary.BigEndian, uint16(len(p)))
	if err != nil {
		return
	}
	return c.ExtendedConn.Write(p)
}

func (c *serverPacketConn) ReadPacket(buffer *buf.Buffer) (destination M.Socksaddr, err error) {
	var packetLen uint16
	err = binary.Read(c.ExtendedConn, binary.BigEndian, &packetLen)
	if err != nil {
		return
	}

	_, err = buffer.ReadFullFrom(c.ExtendedConn, int(packetLen))
	if err != nil {
		return
	}

	destination = c.destination
	return
}

func (c *serverPacketConn) WritePacket(buffer *buf.Buffer, destination M.Socksaddr) error {
	if !c.responseWritten {
		_, err := c.ExtendedConn.Write([]byte{Version, 0})
		if err != nil {
			return err
		}
		c.responseWritten = true
	}
	packetLen := buffer.Len()
	binary.BigEndian.PutUint16(buffer.ExtendHeader(2), uint16(packetLen))
	return c.ExtendedConn.WriteBuffer(buffer)
}

func (c *serverPacketConn) FrontHeadroom() int {
	return 2
}

func (c *serverPacketConn) NeedAdditionalReadDeadline() bool {
	return true
}

func (c *serverPacketConn) Upstream() any {
	return c.ExtendedConn
}
