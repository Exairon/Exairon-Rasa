import asyncio
import inspect
import json
import logging
from asyncio import Queue, CancelledError
from sanic import Blueprint, response
from sanic.request import Request
from sanic.response import HTTPResponse
from typing import Text, Dict, Any, Optional, Callable, Awaitable, NoReturn, List

import rasa.utils.endpoints
from rasa.core.channels.channel import (
    InputChannel,
    CollectingOutputChannel,
    UserMessage,
)


logger = logging.getLogger(__name__)


class RestInput(InputChannel):
    """A custom http input channel.

    This implementation is the basis for a custom implementation of a chat
    frontend. You can customize this to send messages to Rasa and
    retrieve responses from the assistant."""

    @classmethod
    def name(cls) -> Text:
        return "rest"

    @staticmethod
    async def on_message_wrapper(
        on_new_message: Callable[[UserMessage], Awaitable[Any]],
        text: Text,
        queue: Queue,
        sender_id: Text,
        input_channel: Text,
        metadata: Optional[Dict[Text, Any]],
    ) -> None:
        collector = QueueOutputChannel(queue)

        message = UserMessage(
            text, collector, sender_id, input_channel=input_channel, metadata=metadata
        )
        await on_new_message(message)

        await queue.put("DONE")

    def get_metadata(self, request: Request) -> Dict[Text, Any]:
        """Extracts the metadata 
        Args:
            request: A `Request` object that contains Exairon Messenger Metadata.
        Returns:
            Metadata extracted from the sent event payload.
        """
        em_event = request.json
        em_message_id = em_event.get("em_message_id", {})
        em_metadata = em_event.get("em_metadata", {})

        return {
            "em_message_id": em_message_id,
            "em_metadata": em_metadata,
        }

    async def _extract_sender(self, req: Request) -> Optional[Text]:
        return req.json.get("sender", None)

    # noinspection PyMethodMayBeStatic
    def _extract_message(self, req: Request) -> Optional[Text]:
        return req.json.get("message", None)

    def _extract_input_channel(self, req: Request) -> Text:
        return req.json.get("input_channel") or self.name()

    def _extract_output_channel(self, req: Request) -> Text:
        return req.json.get("output_channel") or self.name()

    def stream_response(
        self,
        on_new_message: Callable[[UserMessage], Awaitable[None]],
        text: Text,
        sender_id: Text,
        input_channel: Text,
        metadata: Optional[Dict[Text, Any]],
    ) -> Callable[[Any], Awaitable[None]]:
        async def stream(resp: Any) -> None:
            q = Queue()
            task = asyncio.ensure_future(
                self.on_message_wrapper(
                    on_new_message, text, q, sender_id, input_channel, metadata
                )
            )
            while True:
                result = await q.get()
                if result == "DONE":
                    break
                else:
                    await resp.write(json.dumps(result) + "\n")
            await task

        return stream

    def blueprint(
        self, on_new_message: Callable[[UserMessage], Awaitable[None]]
    ) -> Blueprint:
        custom_webhook = Blueprint(
            "custom_webhook_{}".format(type(self).__name__),
            inspect.getmodule(self).__name__,
        )

        # noinspection PyUnusedLocal
        @custom_webhook.route("/", methods=["GET"])
        async def health(request: Request) -> HTTPResponse:
            return response.json({"status": "ok"})

        @custom_webhook.route("/webhook", methods=["POST"])
        async def receive(request: Request) -> HTTPResponse:
            sender_id = await self._extract_sender(request)
            text = self._extract_message(request)
            should_use_stream = rasa.utils.endpoints.bool_arg(
                request, "stream", default=False
            )
            input_channel = self._extract_input_channel(request)
            output_channel = self._extract_output_channel(request)
            metadata = self.get_metadata(request)

            if should_use_stream:
                return response.stream(
                    self.stream_response(
                        on_new_message, text, sender_id, input_channel, metadata
                    ),
                    content_type="text/event-stream",
                )
            else:
                collector = ExaironOutputChannel(output_channel)

                message = UserMessage(
                    text,
                    collector,
                    sender_id,
                    input_channel=input_channel,
                    metadata=metadata,
                    message_id=metadata['em_message_id']
                )

                # noinspection PyBroadException
                try:
                    await on_new_message(
                        message
                    )
                except CancelledError:
                    logger.error(
                        f"Message handling timed out for " f"user message '{text}'."
                    )
                except Exception:
                    logger.exception(
                        f"An exception occured while handling "
                        f"user message '{text}'."
                    )

                for m in collector.messages:
                    m['em_message_id'] = message.metadata['em_message_id']

                return response.json(collector.messages)

        return custom_webhook


class QueueOutputChannel(CollectingOutputChannel):
    """Output channel that collects send messages in a list

    (doesn't send them anywhere, just collects them)."""

    @classmethod
    def name(cls) -> Text:
        return "queue"

    # noinspection PyMissingConstructor
    def __init__(self, message_queue: Optional[Queue] = None) -> None:
        super().__init__()
        self.messages = Queue() if not message_queue else message_queue

    def latest_output(self) -> NoReturn:
        raise NotImplementedError("A queue doesn't allow to peek at messages.")

    async def _persist_message(self, message: Dict[Text, Any]) -> None:
        await self.messages.put(message)

class ExaironOutputChannel(CollectingOutputChannel):
    """Output channel that collects send messages in a list

    (doesn't send them anywhere, just collects them)."""

    # @classmethod
    def name(self) -> Text:
        """Name of ExaironOutputChannel."""
        return self.output_channel or "collector"

    def __init__(self, output_channel: Optional[Text] = None) -> None:
        self.output_channel = output_channel
        super().__init__()

    async def send_response(self, recipient_id: Text, message: Dict[Text, Any]) -> None:
        """Send a message to the client."""

        if message.get("buttons") and message.get("custom"):
            await self.send_text_with_buttons(
                recipient_id, message.pop("text"), message.pop("buttons"), message.pop("custom"), **message
            )
        elif message.get("buttons"):
            await self.send_text_with_buttons(
                recipient_id, message.pop("text"), message.pop("buttons"), None, **message
            )
        elif message.get("image") and message.get("custom"):
            await self.send_image_url(recipient_id, message.pop("image"), message.pop("text"), message.pop("custom"), **message)
        elif message.get("image"):
            await self.send_image_url(recipient_id, message.pop("image"), message.pop("text"), **message)
        elif message.get("text") and message.get("custom"):
            await self.send_text_message(recipient_id, message.pop("text"), message.pop("custom"), **message)
        elif message.get("text"):
            await self.send_text_message(recipient_id, message.pop("text"), **message)

        # if there is an image we handle it separately as an attachment
        # if message.get("image"):
        #     await self.send_image_url(recipient_id, message.pop("image"), **message)

        if message.get("attachment") and message.get("custom"):
            await self.send_attachment(
                recipient_id, message.pop("attachment"), message.pop("custom"), **message
            )
        elif message.get("attachment"):
            await self.send_attachment(
                recipient_id, message.pop("attachment"), **message
            )

        if message.get("custom"):
            await self.send_custom_json(recipient_id, message.pop("custom"), **message)

        if message.get("elements"):
            await self.send_elements(recipient_id, message.pop("elements"), **message)

    async def send_image_url(
        self, recipient_id: Text, image: Text, text: Text, json_message: Optional[Dict[Text, Any]] = None, **kwargs: Any
    ) -> None:
        """Sends an image. Default will just post the url as a string."""
        if (text and json_message):
            await self._persist_message(self._message(recipient_id, image=image, text=text, custom=json_message))
        elif (text):
            await self._persist_message(self._message(recipient_id, image=image, text=text))
        else:
            await self._persist_message(self._message(recipient_id, image=image))

    async def send_text_with_buttons(
        self,
        recipient_id: Text,
        text: Text,
        buttons: List[Dict[Text, Any]],
        json_message: Optional[Dict[Text, Any]] = None,
        **kwargs: Any,
    ) -> None:
        if (json_message):
            await self._persist_message(self._message(recipient_id, text=text, buttons=buttons, custom=json_message))
        else:
            await self._persist_message(self._message(recipient_id, text=text, buttons=buttons))

    async def send_text_message(
        self, recipient_id: Text, text: Text, json_message: Optional[Dict[Text, Any]] = None, **kwargs: Any
    ) -> None:
        if (json_message):
            await self._persist_message(self._message(recipient_id, text=text, custom=json_message))
        else:
            await self._persist_message(self._message(recipient_id, text=text))
            # for message_part in text.strip().split("\n\n"):
            #     await self._persist_message(self._message(recipient_id, text=message_part))

    async def send_attachment(
        self, recipient_id: Text, attachment: Text, json_message: Optional[Dict[Text, Any]] = None, **kwargs: Any
    ) -> None:
        """Sends an attachment. Default will just post as a string."""
        if (json_message):
            await self._persist_message(self._message(recipient_id, attachment=attachment, custom=json_message))
        else:
            await self._persist_message(self._message(recipient_id, attachment=attachment))
            