package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type RpcHandler interface {
	sendRequestVote(ctx context.Context, addr string, voteInfo []byte) *VoteRequestResponse
	sendAppendEntryRequest(ctx context.Context, addr string, ae *AppendEntries) *AppendEntryResponse
	sendJoinRequest(ctx context.Context, leaderAddr string)
}

func (r *Raft) sendRequestVote(addr string, voteInfo []byte) *VoteRequestResponse {
	voteApi := "http://" + addr + "/requestVote?"
	rsp, err := http.Post(voteApi, "application/json", bytes.NewBuffer(voteInfo))
	if err != nil {
		fmt.Printf("---->[%s], node:[%s] start elect-Post err: %+v \n", time.Now().Format("2006-01-02 15:04:05"), r.curNode, err)
		return nil
	}
	defer rsp.Body.Close()
	b, err := io.ReadAll(rsp.Body)
	if err != nil {
		fmt.Printf("---->[%s], node:[%s] start elect-ReadAll err: %+v \n", time.Now().Format("2006-01-02 15:04:05"), r.curNode, err)
		return nil
	}
	vr := &VoteRequestResponse{}
	err = vr.Decode(b)
	if err != nil {
		fmt.Printf("---->[%s], node:[%s] start elect-Decode err: %+v, %s \n", time.Now().Format("2006-01-02 15:04:05"), r.curNode, err, string(b))
		return nil
	}
	return vr
}

func (r *Raft) HandleVote(w http.ResponseWriter, req *http.Request) {

	// 读取请求体中的数据
	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	// 解析 JSON 数据
	var voteReq RequestVote
	err = json.Unmarshal(body, &voteReq)
	if err != nil {
		http.Error(w, "Failed to parse JSON data", http.StatusBadRequest)
		return
	}
	fmt.Printf("----->>>> noode[%s] get vote request, %+v \n", r.curNode, voteReq)
	ev := &event{c: make(chan error, 1), payload: &voteReq}
	r.evChan <- ev

	err = <-ev.c
	if err != nil {
		fmt.Println("----> handle request vote return err", err.Error())
	}
	_, _ = w.Write(ev.returnValue)
}

// sendAppendEntryRequest
func (r *Raft) sendAppendEntryRequest(addr string, ae *AppendEntries) *AppendEntryResponse {
	aeApi := "http://" + addr + "/appendEntries"
	jsonData, _ := ae.Json()
	resp, err := http.Post(aeApi, "application/json", bytes.NewBuffer(jsonData))
	if err != nil || resp == nil {
		fmt.Println("sendAppendEntryRequest->Post", resp, err)
		return nil
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("sendAppendEntryRequest->ReadAll", data, err)
		return nil
	}
	var rsp AppendEntryResponse
	err = rsp.Decode(data)
	if err != nil {
		fmt.Println("sendAppendEntryRequest->Decode", rsp, err, string(data))
		return nil
	}
	return &rsp
}

func (r *Raft) HandleAppendEntryRequest(w http.ResponseWriter, req *http.Request) {
	// 读取请求体中的数据
	body, err := io.ReadAll(req.Body)
	if err != nil {
		fmt.Printf("!!!----->>>> noode[%s] get append entry request, %+v, %+v, \n", r.curNode, string(body), err)
		http.Error(w, "Failed to read request body", http.StatusInternalServerError)
		return
	}

	// 解析 JSON 数据
	var aeReq AppendEntries
	err = json.Unmarshal(body, &aeReq)
	if err != nil {
		fmt.Printf("!!!----->>>> noode[%s] get append entry request, %+v, %+v,%+v \n", r.curNode, aeReq, string(body), err)
		http.Error(w, "Failed to parse JSON data", http.StatusBadRequest)
		return
	}

	fmt.Printf("----->>>> noode[%s] get append entry request, %+v, %d \n", r.curNode, aeReq, len(aeReq.Entries))
	ev := &event{payload: &aeReq, c: make(chan error, 1)}
	r.evChan <- ev
	err = <-ev.c
	if err != nil {
		fmt.Println("--->HandleAppendEntryRequest", err.Error())
	}
	_, _ = w.Write(ev.returnValue)
}

func (r *Raft) sendJoinRequest(leaderAddr string) {
	req := &JoinRequest{
		ServId:   r.servID,
		AddrInfo: fmt.Sprintf("%s:%d", r.host, r.port),
	}
	jsonData, _ := req.Encode()
	fmt.Println("--->sendJoinRequest", req)
	joinApi := "http://" + leaderAddr + "/join"
	_, err := http.Post(joinApi, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("---->[%s], node:[%s] start sendJoinRequest-Post err: %+v \n", time.Now().Format("2006-01-02 15:04:05"), r.curNode, err)
		return
	}
}

func (r *Raft) HandleJoinRequest(w http.ResponseWriter, req *http.Request) {
	// 读取请求体中的数据
	body, err := io.ReadAll(req.Body)
	if err != nil {
		fmt.Println("Failed to read request body")
		return
	}

	// 解析 JSON 数据
	var joinReq JoinRequest
	err = json.Unmarshal(body, &joinReq)
	if err != nil {
		fmt.Println("Failed to parse JSON data")
		return
	}

	fmt.Printf("----->>>> noode[%s] get sendJoinRequest request, %+v, %s \n", r.curNode, joinReq, string(body))
	cmd := &joinCommand{c: make(chan error, 1), p: &joinReq}
	r.cmdChan <- cmd

	fmt.Printf("----->>>> noode[%s] get sendJoinRequest request after, %+v, %d \n", r.curNode, joinReq, len(r.cmdChan))
	err = <-cmd.c
	if err != nil {
		fmt.Println("----> get sendJoinRequest request return err", err.Error())
	}
	//_, _ = w.Write(cmd.returnValue)
}
