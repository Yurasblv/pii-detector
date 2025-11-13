import os

import mitie  # type: ignore
from loguru import logger


class MitieService:
    MIN_SCORE = 0.2
    RECOGNIZER_INDEX = 0

    def __init__(self):  # type: ignore
        ner_model_location = os.path.abspath(__file__ + f"/../../../" + "mitie_model.dat")
        self.ner_model = mitie.named_entity_extractor(ner_model_location)

    def extract_entities(self, text: str) -> list[tuple[int, str, float]]:
        tokens = mitie.tokenize(text)
        entities = self.ner_model.extract_entities(tokens)
        calculated_len = 0
        results = []
        try:
            for entity_range, entity_type, score in entities:
                if entity_type != "PERSON" or round(score, 1) < 0.8:
                    continue

                entity_text = " ".join(tokens[i].decode('utf-8') for i in entity_range)
                start = calculated_len + text.find(entity_text)
                end = start + len(entity_text)
                calculated_len = end
                # if there are few same entities in text, text will be cut after each entity
                text = text[end:]
                results.append((self.RECOGNIZER_INDEX, entity_text, score))
        except Exception as e:
            logger.warning(f"Hyperscan scanning failed: {e}")
        return results


mitie_service = MitieService()  # type: ignore
