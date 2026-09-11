package runnerclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/JDinSeattle/forge-runtime/internal/domain"
	"github.com/JDinSeattle/forge-runtime/internal/runner"
	pb "github.com/JDinSeattle/forge-runtime/proto/runner/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type TLSFiles struct {
	CertFile        string `json:"cert_file"`
	KeyFile         string `json:"key_file"`
	CAFile          string `json:"ca_file"`
	ExpectedPeerURI string `json:"expected_peer_uri"`
	ServerName      string `json:"server_name,omitempty"`
}
type ServerConfig struct {
	// OperatorFault is local executable-only instrumentation, never JSON/RPC input.
	OperatorFault    func(string, runner.OperationRequest) error `json:"-"`
	UnixSocket       string                                      `json:"unix_socket,omitempty"`
	TCPAddress       string                                      `json:"tcp_address,omitempty"`
	TLS              TLSFiles                                    `json:"tls"`
	MaxMessageBytes  int                                         `json:"max_message_bytes,omitempty"`
	MaxRPCTime       time.Duration                               `json:"max_rpc_time,omitempty"`
	MaxOperationTime time.Duration                               `json:"max_operation_time,omitempty"`
}
type Server struct {
	server   *grpc.Server
	listener net.Listener
	done     chan error
}

func (s *Server) Address() string { return s.listener.Addr().String() }
func (s *Server) Wait() error     { return <-s.done }
func (s *Server) Close(ctx context.Context) error {
	stopped := make(chan struct{})
	go func() { s.server.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		return ctx.Err()
	}
}

func Serve(config ServerConfig, service runner.Service) (*Server, error) {
	if service == nil || (config.UnixSocket == "") == (config.TCPAddress == "") {
		return nil, fmt.Errorf("%w: choose exactly one runner transport", domain.ErrInvalid)
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 8 << 20
	}
	if config.MaxMessageBytes < 1024 || config.MaxMessageBytes > 64<<20 {
		return nil, domain.ErrInvalid
	}
	if config.MaxRPCTime == 0 {
		config.MaxRPCTime = 30 * time.Second
	}
	if config.MaxRPCTime < time.Millisecond || config.MaxRPCTime > time.Minute {
		return nil, domain.ErrInvalid
	}
	if config.MaxOperationTime == 0 {
		config.MaxOperationTime = 10 * time.Minute
	}
	if config.MaxOperationTime < time.Second || config.MaxOperationTime > time.Hour {
		return nil, domain.ErrInvalid
	}
	options := []grpc.ServerOption{grpc.MaxRecvMsgSize(config.MaxMessageBytes), grpc.MaxSendMsgSize(config.MaxMessageBytes), grpc.MaxConcurrentStreams(64), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, cancel := context.WithTimeout(ctx, config.MaxRPCTime)
		defer cancel()
		result, err := handler(ctx, req)
		return result, rpcError(err)
	})}
	var listener net.Listener
	var err error
	if config.UnixSocket != "" {
		if !filepath.IsAbs(config.UnixSocket) {
			return nil, domain.ErrInvalid
		}
		dir := filepath.Dir(config.UnixSocket)
		if err = os.MkdirAll(dir, 0700); err != nil {
			return nil, err
		}
		info, err := os.Stat(dir)
		if err != nil {
			return nil, err
		}
		if info.Mode().Perm()&0077 != 0 {
			return nil, fmt.Errorf("%w: unix socket directory must be private", domain.ErrForbidden)
		}
		// Never unlink an existing path automatically; a stale socket is an
		// operator-visible startup conflict, avoiding deletion of a live listener.
		if _, err = os.Lstat(config.UnixSocket); err == nil {
			return nil, fmt.Errorf("%w: unix socket already exists", domain.ErrConflict)
		} else if !os.IsNotExist(err) {
			return nil, err
		}
		listener, err = net.Listen("unix", config.UnixSocket)
		if err != nil {
			return nil, err
		}
		if err = os.Chmod(config.UnixSocket, 0600); err != nil {
			_ = listener.Close()
			return nil, err
		}
	} else {
		tlsConfig, err := loadTLS(config.TLS, true)
		if err != nil {
			return nil, err
		}
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig)))
		listener, err = net.Listen("tcp", config.TCPAddress)
		if err != nil {
			return nil, err
		}
	}
	server := grpc.NewServer(options...)
	pb.RegisterRunnerServiceServer(server, &rpcServer{service: service, fault: config.OperatorFault, maxOperationTime: config.MaxOperationTime})
	s := &Server{server: server, listener: listener, done: make(chan error, 1)}
	go func() { s.done <- server.Serve(listener); close(s.done) }()
	return s, nil
}

func loadTLS(files TLSFiles, server bool) (*tls.Config, error) {
	if files.CertFile == "" || files.KeyFile == "" || files.CAFile == "" || files.ExpectedPeerURI == "" {
		return nil, fmt.Errorf("%w: remote runner requires mutual TLS and explicit peer identity", domain.ErrInvalid)
	}
	identity, err := url.Parse(files.ExpectedPeerURI)
	if err != nil || identity.Scheme != "spiffe" || identity.Host == "" || identity.User != nil || identity.RawQuery != "" || identity.Fragment != "" {
		return nil, domain.ErrInvalid
	}
	certificate, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(files.CAFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(data) {
		return nil, domain.ErrInvalid
	}
	config := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, ServerName: files.ServerName}
	if server {
		config.ClientCAs = roots
		config.ClientAuth = tls.RequireAndVerifyClientCert
	} else if files.ServerName == "" {
		return nil, fmt.Errorf("%w: TLS server_name is required", domain.ErrInvalid)
	}
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
			return fmt.Errorf("unverified TLS peer")
		}
		for _, uri := range state.PeerCertificates[0].URIs {
			if uri.String() == files.ExpectedPeerURI {
				return nil
			}
		}
		return fmt.Errorf("TLS peer URI does not match required service identity")
	}
	return config, nil
}
