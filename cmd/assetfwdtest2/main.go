package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/lightningnetwork/lnd/macaroons"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/macaroon.v2"
)

func main() {
	if len(os.Args) < 6 {
		fmt.Println("Usage: asset_flow_test <lnd_host:port> <tls_cert> <macaroon> <payment_hash_hex> <payment_req>")
		os.Exit(1)
	}

	lndHost := os.Args[1]
	tlsCertPath := os.Args[2]
	macaroonPath := os.Args[3]
	paymentHashHex := os.Args[4]
	paymentReq := os.Args[5]

	creds, err := credentials.NewClientTLSFromFile(tlsCertPath, "")
	if err != nil {
		fmt.Printf("TLS error: %v\n", err)
		os.Exit(1)
	}

	macBytes, err := os.ReadFile(macaroonPath)
	if err != nil {
		fmt.Printf("Macaroon error: %v\n", err)
		os.Exit(1)
	}

	mac := &macaroon.Macaroon{}
	if err := mac.UnmarshalBinary(macBytes); err != nil {
		fmt.Printf("Macaroon unmarshal error: %v\n", err)
		os.Exit(1)
	}

	cred, err := macaroons.NewMacaroonCredential(mac)
	if err != nil {
		fmt.Printf("Macaroon credential error: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, lndHost,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(cred),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Printf("Connect error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	routerClient := routerrpc.NewRouterClient(conn)

	paymentHash, err := hex.DecodeString(paymentHashHex)
	if err != nil {
		fmt.Printf("Payment hash decode error: %v\n", err)
		os.Exit(1)
	}

	// Build route using QueryRoutes via CLI since our manual route fails
	// Let's first just verify the Route is valid by sending it

	node2Key := "02c587bea0bc23885ada9bc00b02e5786ccdff5d60be7b7bae746d7c2d95283218"
	node3Key := "021fca4324c2af3962d0a7971546e240c9240590abbad54b136a40e69284a188fd"
	node4Key := "0308b60f3ff70e892b4e911a0b10d70be6dfb72d7a08550a243686db28e8662b8f"

	// Use 3 BTC channels (control test)
	btcChan12 := uint64(135818273332723712)
	btcChan23 := uint64(135822671379234816)
	btcChan34 := uint64(135829268449001472)

	assetChan12 := uint64(135847960146673664)
	assetChan23 := uint64(135847960146804736)
	assetChan34 := uint64(135847960146739200)

	// Mode selection
	useAssetChain := len(os.Args) > 6 && os.Args[6] == "asset"

	var hop0scid, hop1scid, hop2scid uint64
	label := "BTC chain"
	if useAssetChain {
		hop0scid = assetChan12
		hop1scid = assetChan23
		hop2scid = assetChan34
		label = "ASSET chain"
	} else {
		hop0scid = btcChan12
		hop1scid = btcChan23
		hop2scid = btcChan34
	}

	useTLV := len(os.Args) > 7 && os.Args[7] == "tlv"

	// Amount: 500 sat
	hop2Amt := int64(500)
	hop2Msat := int64(500000)
	hop1Amt := hop2Amt + 1
	hop1Msat := hop1Amt * 1000
	hop0Amt := hop1Amt + 1
	hop0Msat := hop0Amt * 1000

	assetIDHex := "419cb727f0c9569bd2f62bb8dddc32b0ccd2f6d0513518ec5b5417ae60dba29d"
	assetID, _ := hex.DecodeString(assetIDHex)
	assetAmt := uint64(500000)

	fmt.Printf("=== SendToRouteV2: %s (TLV=%v) ===\n", label, useTLV)

	// Build hops
	hops := make([]*lnrpc.Hop, 3)

	cr0 := make(map[uint64][]byte)
	if useTLV {
		cr0[65536] = uint64ToTLVBytes(assetAmt)
		cr0[65537] = assetID
		cr0[65539] = []byte{0x01}
	}
	hops[0] = &lnrpc.Hop{
		ChanId:           hop0scid,
		AmtToForward:     hop0Amt,
		AmtToForwardMsat: hop0Msat,
		Expiry:           200,
		PubKey:           node2Key,
		CustomRecords:    cr0,
	}

	cr1 := make(map[uint64][]byte)
	if useTLV {
		cr1[65536] = uint64ToTLVBytes(assetAmt)
		cr1[65537] = assetID
		cr1[65539] = []byte{0x01}
	}
	hops[1] = &lnrpc.Hop{
		ChanId:           hop1scid,
		AmtToForward:     hop1Amt,
		AmtToForwardMsat: hop1Msat,
		Expiry:           140,
		PubKey:           node3Key,
		CustomRecords:    cr1,
	}

	cr2 := make(map[uint64][]byte)
	if useTLV {
		cr2[65536] = uint64ToTLVBytes(assetAmt)
		cr2[65537] = assetID
	}
	hops[2] = &lnrpc.Hop{
		ChanId:           hop2scid,
		AmtToForward:     hop2Amt,
		AmtToForwardMsat: hop2Msat,
		Expiry:           80,
		PubKey:           node4Key,
		CustomRecords:    cr2,
		MppRecord: &lnrpc.MPPRecord{
			TotalAmtMsat: hop2Msat,
			PaymentAddr:  make([]byte, 32), // placeholder
		},
	}

	// Calculate total time lock: worst case = first hop expiry + some buffer
	// First hop has expiry=200, blockHeight ~126739
	totalTimeLock := uint32(126739 + 200)

	route := &lnrpc.Route{
		Hops:          hops,
		TotalAmt:      hop0Amt,
		TotalAmtMsat:  hop0Msat,
		TotalFees:     hop0Amt - hop2Amt,
		TotalFeesMsat: hop0Msat - hop2Msat,
		TotalTimeLock: totalTimeLock,
	}

	fmt.Printf("  Amt=%d sat (%d msat) | Total=%d sat (%d msat)\n",
		hop2Amt, hop2Msat, hop0Amt, hop0Msat)
	for i, h := range route.Hops {
		tlvInfo := ""
		if useTLV {
			tlvInfo = fmt.Sprintf(" TLV=[65536,65537%s]",
				map[bool]string{true:",65539"}[i < 2])
		}
		fmt.Printf("  Hop %d: chan=%d amt=%d msat%s\n",
			i, h.ChanId, h.AmtToForwardMsat, tlvInfo)
	}

	// Set the payment hash from invoice
	payReq, err := NewLightningClient(conn).DecodePayReq(ctx, &lnrpc.PayReqString{PayReq: paymentReq})
	if err == nil && payReq != nil {
		route.Hops[2].MppRecord.PaymentAddr = payReq.PaymentAddr
		route.Hops[2].MppRecord.TotalAmtMsat = payReq.NumMsat
	}

	lnClient := NewLightningClient(conn)
	info, _ := lnClient.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	_ = info

	sendResp, err := routerClient.SendToRouteV2(ctx, &routerrpc.SendToRouteRequest{
		PaymentHash: paymentHash,
		Route:       route,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	fmt.Printf("Status: %v\n", sendResp.Status)
	if sendResp.Failure != nil {
		fmt.Printf("FailureCode: %d, Source: %d\n",
			sendResp.Failure.Code, sendResp.Failure.FailureSourceIndex)
	}
	if sendResp.Status == lnrpc.HTLCAttempt_SUCCEEDED {
		fmt.Printf("✅ PAYMENT SUCCEEDED!\n")
	}
}

func NewLightningClient(conn *grpc.ClientConn) lnrpc.LightningClient {
	return lnrpc.NewLightningClient(conn)
}

func uint64ToTLVBytes(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	zeros := 0
	for zeros < 7 && b[zeros] == 0 {
		zeros++
	}
	return b[zeros:]
}
