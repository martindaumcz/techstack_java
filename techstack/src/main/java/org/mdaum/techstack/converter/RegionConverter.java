package org.mdaum.techstack.converter;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.regions.Region;

public class RegionConverter implements Converter<String, Region> {

    @Override
    public Region convert(String regionString) {
        return Region.of(regionString);
    }

}
