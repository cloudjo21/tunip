from itertools import takewhile

from .entity_sets import MetaSourcedEntitySet


class MetaSourcedEntitySetMerger:

    @classmethod
    def merge(cls, a_entity_set: MetaSourcedEntitySet, b_entity_set: MetaSourcedEntitySet) -> MetaSourcedEntitySet:
        merged = []

        a_entity_set.sort()
        b_entity_set.sort()

        iter_entity_a = iter(a_entity_set)
        iter_entity_b = iter(b_entity_set)
        entity_a = next(iter_entity_a)
        entity_b = next(iter_entity_b)
        has_next_of_a = True if iter_entity_a else False
        has_next_of_b = True if iter_entity_b else False


        while has_next_of_a or has_next_of_b:
            if entity_a.lexical == entity_b.lexical:
                entity_a.update_meta(entity_b)
                merged.append(entity_a)

            b_entities_above_a = takewhile(lambda ent: ent.lexical != entity_a, iter_entity_b)
            if b_entities_above_a:
                merged.extend(b_entities_above_a)
                entity_b = next(iter_entity_b, None)
                has_next_of_b = True if entity_b else False
                if entity_b and entity_a:
                    entity_b.update_meta(entity_a)
                    merged.append(entity_b)

            a_entities_above_b = takewhile(lambda ent: ent.lexical != entity_b, iter_entity_a)
            if a_entities_above_b:
                merged.extend(a_entities_above_b)
                entity_a = next(iter_entity_a,  None)
                has_next_of_a = True if entity_a else False
                if entity_a and entity_b:
                    entity_a.update_meta(entity_b)
                    merged.append(entity_a)
        
        return merged
