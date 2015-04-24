using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;

namespace MessageVault {

	/// <summary>
	///     Is responsible for passing non-persisted messages
	/// </summary>
	public static class TransferFormat {

		public const byte FormatVersion = 1;
		public static void WriteMessages(ICollection<Message> messages, Stream stream) {
			using (var bin = new BinaryWriter(stream, Encoding.UTF8, true)) {
				// int
				
				bin.Write(FormatVersion);
				bin.Write((ushort) messages.Count);


				foreach (var message in messages) {
					bin.Write(message.Attributes);
					// we know for 100% that key length will be byte 
					bin.Write((byte) message.Key.Length);
					bin.Write(message.Key);
					// we know for 100% that value length will be ushort
					bin.Write((ushort) message.Value.Length); //int32
					bin.Write(message.Value);
					bin.Write(message.Crc32);
				}
				stream.Seek(0, SeekOrigin.Begin);
				using (var md5 = new MD5CryptoServiceProvider()) {
					md5.ComputeHash(stream);

					//Debug.WriteLine("Writing {1} at {0}", stream.Position, BitConverter.ToString(md5.Hash));
					bin.Write(md5.Hash);
				}
			}
		}

		public static Message[] ReadMessages(Stream source) {
			using (var md = new MD5CryptoServiceProvider()) {
				using (var crypto = new CryptoStream(source, md, CryptoStreamMode.Read)) {
					using (var bin = new BinaryReader(crypto, Encoding.UTF8, true)) {

						var version = bin.ReadByte();
						if (version != FormatVersion) {
							throw new InvalidOperationException("Unexpected transfer format version");
						}

						var count = bin.ReadUInt16();
						var result = new Message[count];
						for (var i = 0; i < count; i++) {
							var flags = bin.ReadByte();
							var keyLength = bin.ReadByte();
							var key = bin.ReadBytes(keyLength);
							var valueLength = bin.ReadUInt16();
							var value = bin.ReadBytes(valueLength);
							var crc = bin.ReadUInt32();

							var messageToWrite = new Message(flags, key, value, crc);

							result[i] = messageToWrite;
						}

						crypto.FlushFinalBlock();

						var computed = md.Hash;

						//Debug.WriteLine("Expecting to read {0} at {1}", BitConverter.ToString(computed), source.Position);
						var hash = new byte[16];
						source.Read(hash, 0, 16);
						VerifyHash(computed, hash);

						return result;
					}
				}
			}
		}

		static void VerifyHash(byte[] computed, byte[] actual) {
			for (int i = 0; i < 16; i++) {
				if (computed[i] != actual[i]) {
					throw new InvalidDataException("Data transfer failure");
				}
			}
		}
	}

}