 /*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 * 
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 * 
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 * 
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS 
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF 
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "channel/all_includes.hpp"
#include "util/datagram_packet.hpp"

using namespace comms;

//static boost::object_pool<HeapByteArray> kByteArrayPool;

int32 DatagramChannel::Send(const SocketInetAddress& addr, Buffer* buffer)
{
	char* raw = const_cast<char*>(buffer->GetRawReadBuffer());
	uint32 len = buffer->ReadableBytes();
	int ret = ::sendto(GetSocketFD(addr.GetDomain()), raw, len, 0,
			&(addr.GetRawSockAddr()), addr.GetRawSockAddrSize());
	if (ret > 0)
	{
		buffer->AdvanceReadIndex(len);
	}
	return ret;
}

int32 DatagramChannel::Receive(SocketInetAddress& addr, Buffer* buffer)
{
	sockaddr& tempaddr = const_cast<sockaddr&>(addr.GetRawSockAddr());
	socklen_t socklen = sizeof(sockaddr);
	memset(&tempaddr, 0, sizeof(struct sockaddr_in));
	buffer->EnsureWritableBytes(65536);
	char* raw = const_cast<char*>(buffer->GetRawWriteBuffer());
	int ret = ::recvfrom(GetSocketFD(addr.GetDomain()), raw,
			buffer->WriteableBytes(), 0, &tempaddr, &socklen);
	if (ret > 0)
	{
		buffer->AdvanceWriteIndex(ret);
	}
	return ret;
}

void DatagramChannel::OnRead()
{
	DatagramPacket packet(65536);
	int32 len = Receive(packet.GetInetAddress(), &(packet.GetBuffer()));
	if (len > 0)
	{
		fire_message_received<DatagramPacket>(this, &packet, NULL);
	}
}

DatagramChannel::~DatagramChannel()
{
	//m_service.GetEventChannelService().DestroyEventChannel(m_event_channel);
}