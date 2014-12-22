namespace MessageVault.Api {

	public sealed class GetStreamResponse
	{
		public string Signature { get; set; }
	}

	public sealed class PostMessagesResponse {
		public long Position { get; set; }
	}

	public sealed class ErrorResponse {
		public string Error { get; set; }
		public string Type { get; set; }
	}

}