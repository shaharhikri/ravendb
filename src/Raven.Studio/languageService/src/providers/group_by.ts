import { CandidatesCollection } from "antlr4-c3/out/src/CodeCompletionCore";
import { BaseAutocompleteProvider } from "./baseProvider";
import { Scanner } from "../scanner";
import { META_FIELD, META_FUNCTION, SCORING_FIELD, SCORING_FUNCTION } from "./scoring";
import { AutocompleteProvider } from "./common";
import { RqlParser } from "../RqlParser";
import { ProgContext } from "../generated/BaseRqlParser";

export class AutocompleteGroupBy extends BaseAutocompleteProvider implements AutocompleteProvider {
    
    canCompleteArray(candidates: CandidatesCollection) {
        return !candidates.rules.has(RqlParser.RULE_function);
    }
    
    async collectAsync(scanner: Scanner, candidates: CandidatesCollection, parser: RqlParser, parseTree: ProgContext, writtenPart: string): Promise<autoCompleteWordList[]> {
        const rule = AutocompleteGroupBy.findFirstRule(candidates, [RqlParser.RULE_function, RqlParser.RULE_variable, RqlParser.RULE_identifiersAllNames]);
               
        if (rule) {
            const matchedRule = rule[1];
            const inGroupBy = matchedRule.ruleList.length >= 2 && matchedRule.ruleList[0] === RqlParser.RULE_prog && matchedRule.ruleList[1] === RqlParser.RULE_groupByStatement;

            if (!inGroupBy) {
                return [];
            }
            
            const arrayFunction: autoCompleteWordList = {
                score: SCORING_FUNCTION,
                caption: "array()",
                value: "array(",
                meta: META_FUNCTION
            };
            
            const fields = await this.getPossibleFields(parseTree, writtenPart);
            
            const mappedFields = fields.map(f => ({
                meta: META_FIELD,
                score: SCORING_FIELD,
                value: f,
                caption: f
            }));
            
            const result: autoCompleteWordList[] = [];

            if (this.canCompleteArray(candidates)) {
                result.push(arrayFunction);
            }
            
            result.push(...mappedFields);
            
            return result;
        }
        
        return [];
    }
}
