import threading
from collections import defaultdict

class Acceptor:
    def __init__(self):
        self.promised_id = defaultdict(lambda: None)  # Última promessa feita
        self.accepted_id = defaultdict(lambda: None)  # Número da última proposta aceita
        self.accepted_value = defaultdict(lambda: None)  # Valor da última proposta aceita

    def receive_prepare(self, slot, proposal_id):
        if self.promised_id[slot] is None or proposal_id > self.promised_id[slot]:
            self.promised_id[slot] = proposal_id
            return True, self.accepted_id[slot], self.accepted_value[slot]
        return False, None, None

    def receive_accept_request(self, slot, proposal_id, value):
        if self.promised_id[slot] is None or proposal_id >= self.promised_id[slot]:
            self.promised_id[slot] = proposal_id
            self.accepted_id[slot] = proposal_id
            self.accepted_value[slot] = value
            return True
        return False


class Proposer:
    def __init__(self, proposer_id, proposal_id, quorum_size, acceptors):
        self.proposer_id = proposer_id
        self.proposal_id = proposal_id
        self.quorum_size = quorum_size
        self.acceptors = acceptors

    def propose(self, slot, value):
        self.proposal_id += 1
        promises = 0
        highest_accepted_id = -1
        highest_accepted_value = None

        # Phase 1: Send Prepare
        for acceptor in self.acceptors:
            success, last_id, last_value = acceptor.receive_prepare(slot, self.proposal_id)
            if success:
                promises += 1
                if last_id is not None and last_id > highest_accepted_id:
                    highest_accepted_id = last_id
                    highest_accepted_value = last_value

        if promises < self.quorum_size:
            print(f"Proposer {self.proposer_id}: Failed to reach quorum in Prepare phase for slot {slot}.")
            return False

        # Reuse the highest accepted value if any
        if highest_accepted_value is not None:
            print(f"Proposer {self.proposer_id}: Reusing value '{highest_accepted_value}' from proposal {highest_accepted_id}.")
            value = highest_accepted_value

        # Phase 2: Send Accept Request
        accepts = 0
        for acceptor in self.acceptors:
            if acceptor.receive_accept_request(slot, self.proposal_id, value):
                accepts += 1

        if accepts >= self.quorum_size:
            print(f"Proposer {self.proposer_id}: Consensus reached for slot {slot} with value '{value}'.")
            return True
        else:
            print(f"Proposer {self.proposer_id}: Failed to reach quorum in Accept phase for slot {slot}.")
            return False


# Simulação com Reaproveitamento de Valores
if __name__ == "__main__":
    num_acceptors = 5
    quorum_size = (num_acceptors // 2) + 1

    # Criando os Aceitadores
    acceptors = [Acceptor() for _ in range(num_acceptors)]

    def proposer_thread(proposer_id, proposal_id, messages):
        proposer = Proposer(proposer_id=proposer_id, proposal_id = proposal_id, quorum_size=quorum_size, acceptors=acceptors)
        for i, message in enumerate(messages):
            proposer.propose(slot=i, value=message)

    # Proposer tenta propor mensagens para slots específicos
    messages1 = ["Mensagem A1", "Mensagem B1", "Mensagem C1"]
    messages2 = ["Mensagem A2", "Mensagem B2", "Mensagem C2"]

    thread1 = threading.Thread(target=proposer_thread, args=(1, 1, messages1))
    thread2 = threading.Thread(target=proposer_thread, args=(2, 2, messages2))
    thread3 = threading.Thread(target=proposer_thread, args=(3, 3, messages2))
    thread4 = threading.Thread(target=proposer_thread, args=(4, 4, messages2))

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()

    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
