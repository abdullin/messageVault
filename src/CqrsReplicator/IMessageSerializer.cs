using System;
using System.Collections.Generic;
using System.IO;

namespace CqrsReplicator {


	/// <summary>
	/// Joins data serializer and contract mapper
	/// </summary>
	public interface IMessageSerializer
	{
		/// <summary>
		/// Writes the message to a stream.
		/// </summary>
		/// <param name="message">The message.</param>
		/// <param name="type">The type.</param>
		/// <param name="stream">The stream.</param>
		void WriteMessage(object message, Type type, Stream stream);
		/// <summary>
		/// Reads the message from a stream.
		/// </summary>
		/// <param name="stream">The stream.</param>
		/// <returns></returns>
		object ReadMessage(Stream stream);



		MessageAttribute[] ReadAttributes(Stream stream);
		void WriteAttributes(ICollection<MessageAttribute> attributes, Stream stream);

		int ReadCompactInt(Stream stream);
		void WriteCompactInt(int value, Stream steam);
	}

	public struct MessageAttribute
	{
		public readonly string Key;
		public readonly string Value;

		public static readonly MessageAttribute[] Empty = new MessageAttribute[0];

		public MessageAttribute(string key, string value)
		{
			Key = key;
			Value = value;
		}
	}
}