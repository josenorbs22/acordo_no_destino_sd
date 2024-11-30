import threading
import time
from collections import defaultdict, Counter

class Acceptor:
    def __init__(self):
        self.promised_id = defaultdict(lambda: None)  # Promessas por slot
        self.support_counter = defaultdict(Counter) 

    def receive_prepare(self, slot, proposal_id):
        if self.promised_id[slot] is None or proposal_id > self.promised_id[slot]:
            self.promised_id[slot] = proposal_id
            return True
        return False

    def receive_accept_request(self, slot, proposal_id, value):
        if self.promised_id[slot] is None or proposal_id >= self.promised_id[slot]:
            self.promised_id[slot] = proposal_id
            self.support_counter[slot][value] += 1
            return True
        return False

    def get_consensus(self, slot, quorum_size):
        """Retorna o valor com maior suporte, desde que tenha quorum."""
        for value, count in self.support_counter[slot].items():
            if count >= quorum_size:
                return value
        return None


class Proposer:
    def __init__(self, proposer_id, proposal_id, quorum_size, acceptors):
        self.proposer_id = proposer_id
        self.proposal_id = proposal_id
        self.quorum_size = quorum_size
        self.acceptors = acceptors

    def propose(self, slot, value):
        promises = 0

        # Phase 1: Send Prepare
        for acceptor in self.acceptors:
            if acceptor.receive_prepare(slot, self.proposal_id):
                promises += 1

        if promises < self.quorum_size:
            print(f"Proponente {self.proposer_id}: Falha em atingir quorum na Fase de Preparação para o slot {slot}.")
            return False

        # Phase 2: Send Accept Request
        supports = 0
        for acceptor in self.acceptors:
            if acceptor.receive_accept_request(slot, self.proposal_id, value):
                supports += 1

        if supports >= self.quorum_size:
            print(f"Proponente {self.proposer_id}: Atingiu quorum para o slot {slot} com valor '{value}'.")
            return True
        else:
            print(f"Proponente {self.proposer_id}: Falha em atingir quorum na Fase de Aceitação para o slot {slot}.")
            return False


# Simulação de Ordenação de Mensagens
if __name__ == "__main__":
    num_acceptors = 5
    quorum_size = (num_acceptors // 2) + 1
    acceptors = [Acceptor() for _ in range(num_acceptors)]

    def proposer_thread(proposer_id, proposal_id, messages):
        proposer = Proposer(proposer_id=proposer_id, proposal_id = proposal_id, quorum_size=quorum_size, acceptors=acceptors)
        for i, message in enumerate(messages):
            proposer.propose(slot=i, value=message)

    # Proposer tenta propor mensagens para slots específicos
    messages1 = ["A1", "B1", "C1"]
    messages2 = ["A2", "B2", "C2"]

    thread1 = threading.Thread(target=proposer_thread, args=(1, 1, messages1))
    thread2 = threading.Thread(target=proposer_thread, args=(2, 2, messages1))
    thread3 = threading.Thread(target=proposer_thread, args=(3, 3, messages1))
    thread4 = threading.Thread(target=proposer_thread, args=(4, 4, messages2))

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()

    for acceptor in acceptors:
        consensus = acceptor.get_consensus(slot=0, quorum_size=quorum_size)
        if consensus:
            print(f"Consenso atingido: {consensus} no slot 0")
            break

    for acceptor in acceptors:
        consensus = acceptor.get_consensus(slot=1, quorum_size=quorum_size)
        if consensus:
            print(f"Consenso atingido: {consensus} no slot 1")
            break
        
    for acceptor in acceptors:
        consensus = acceptor.get_consensus(slot=2, quorum_size=quorum_size)
        if consensus:
            print(f"Consenso atingido: {consensus} no slot 2")
            break

