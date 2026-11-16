import unittest

from src.autosync_client import (
    MAX_WS_TEXT_FRAME,
    AutoSyncClient,
    WebSocketProtocolError,
    connection_status,
)


class WebSocketFrameTests(unittest.TestCase):
    def parse(self, frame):
        return AutoSyncClient._pop_frame(bytearray(frame))

    def test_valid_text_frame_is_consumed(self):
        buffer = bytearray(b"\x81\x02{}")
        self.assertEqual(AutoSyncClient._pop_frame(buffer), (1, b"{}"))
        self.assertEqual(buffer, bytearray())

    def test_partial_frame_waits_without_mutating_buffer(self):
        buffer = bytearray(b"\x81\x03a")
        self.assertIsNone(AutoSyncClient._pop_frame(buffer))
        self.assertEqual(buffer, bytearray(b"\x81\x03a"))
        buffer.extend(b"bc")
        self.assertEqual(AutoSyncClient._pop_frame(buffer), (1, b"abc"))

    def test_masked_server_frame_is_rejected(self):
        with self.assertRaises(WebSocketProtocolError):
            self.parse(b"\x81\x80")

    def test_fragmented_and_reserved_bit_frames_are_rejected(self):
        for frame in (b"\x01\x00", b"\xc1\x00"):
            with self.subTest(frame=frame), self.assertRaises(WebSocketProtocolError):
                self.parse(frame)

    def test_binary_and_unknown_opcodes_are_rejected(self):
        for frame in (b"\x82\x00", b"\x83\x00"):
            with self.subTest(frame=frame), self.assertRaises(WebSocketProtocolError):
                self.parse(frame)

    def test_noncanonical_extended_lengths_are_rejected(self):
        short_encoded_as_16_bit = b"\x81\x7e\x00\x7d" + b"x" * 125
        long_encoded_as_64_bit = b"\x81\x7f" + (65535).to_bytes(8, "big")
        with self.assertRaises(WebSocketProtocolError):
            self.parse(short_encoded_as_16_bit)
        with self.assertRaises(WebSocketProtocolError):
            self.parse(long_encoded_as_64_bit)

    def test_oversized_text_frame_is_rejected_from_header_only(self):
        declared_length = MAX_WS_TEXT_FRAME + 1
        frame_header = b"\x81\x7f" + declared_length.to_bytes(8, "big")
        with self.assertRaises(WebSocketProtocolError):
            self.parse(frame_header)

    def test_reserved_high_bit_in_64_bit_length_is_rejected(self):
        frame_header = b"\x81\x7f" + (1 << 63).to_bytes(8, "big")
        with self.assertRaises(WebSocketProtocolError):
            self.parse(frame_header)

    def test_control_frames_are_limited_to_125_bytes(self):
        for opcode in (8, 9, 10):
            self.assertEqual(self.parse(bytes((0x80 | opcode, 0))), (opcode, b""))
        oversized_ping = b"\x89\x7e\x00\x7e" + b"x" * 126
        with self.assertRaises(WebSocketProtocolError):
            self.parse(oversized_ping)

    def test_protocol_failure_is_reported_as_blocked(self):
        result = connection_status(WebSocketProtocolError("bad frame"))
        self.assertEqual(result, {"status": "blocked", "detail": "websocket_protocol_error"})


if __name__ == "__main__":
    unittest.main(verbosity=2)