using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Linq;
using LZ4s;

namespace MessageVault.Api {




	/// <summary>
	/// Is responsible for passing
	/// </summary>
	public static class ApiMessageFramer {
		public static byte[] WriteMessages(ICollection<MessageToWrite> messages, Stream stream) {
					using (var bin = new BinaryWriter(stream, Encoding.UTF8, true)) {
						// int
						bin.Write(messages.Count);

						foreach (var message in messages) {
							bin.Write(message.Key);
							bin.Write(message.Value.Length); //int32
							bin.Write(message.Value);
						}
					}

			stream.Seek(0, SeekOrigin.Begin);

			using (var md5 = new MD5CryptoServiceProvider()) {
				md5.ComputeHash(stream);
				return md5.Hash;
			}
		}

		public static MessageToWrite[] ReadMessages(Stream source, byte[] optionalHash)
		{
			if (optionalHash == null) {
				return ReadBody(source);
			}

			using (var hash = new MD5CryptoServiceProvider()) {
				MessageToWrite[] body;
				using (var crypto = new CryptoStream(source, hash, CryptoStreamMode.Read)) {
					body = ReadBody(crypto);
				}

				if (!optionalHash.SequenceEqual(hash.Hash)) {
					throw new InvalidDataException("Hash mismatch");
					
				}
				return body;
			}
		}

		static MessageToWrite[] ReadBody(Stream source) {
			using (var bin = new BinaryReader(source, Encoding.UTF8, true)) {
				var len = bin.ReadInt32();
				var result = new MessageToWrite[len];
				for (int i = 0; i < len; i++) {
					var flags = (MessageFlags) bin.ReadByte();
					var contract = bin.ReadString();
					var size = bin.ReadInt32();
					var data = bin.ReadBytes(size);
					result[i] = new MessageToWrite(flags, contract, data);
				}
				return result;
			}
		}
	}

}