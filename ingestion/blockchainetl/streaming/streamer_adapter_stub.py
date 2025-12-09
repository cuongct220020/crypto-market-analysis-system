class StreamerAdapterStub:

    def open(self):
        pass

    @staticmethod
    def get_current_block_number():
        return 0

    def export_all(self, start_block, end_block):
        pass

    def close(self):
        pass
